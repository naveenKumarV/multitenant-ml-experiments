"""A single-process AdmissionCheck controller for fixed-size batch/v1 Jobs.
Kueue owns suspension and quota; Volcano owns placement. Ready means the
PodGroup contract exists, not that node placement is reserved or atomic.
"""
import argparse
import copy
import hashlib
import json
import uuid
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from kubernetes import client, config
from kubernetes.client.exceptions import ApiException

GROUP, VERSION = 'kueue.x-k8s.io', 'v1beta1'
CHECK = 'prototype-volcano'
CONTROLLER = 'prototype.example.com/volcano-bridge'
ANNOTATION = 'prototype.example.com/attempt'


def condition(w, name):
    return next((c for c in w.get('status', {}).get('conditions', [])
                 if c['type'] == name and c['status'] == 'True'), None)


def reservation_key(w):
    reservation = condition(w, 'QuotaReserved')
    if not reservation or not w.get('status', {}).get('admission'):
        return None
    raw = [w['metadata']['uid'], reservation, w['status']['admission']]
    return hashlib.sha256(json.dumps(raw, sort_keys=True).encode()).hexdigest()[:16]


def attempt(w):
    check = next((c for c in w.get('status', {}).get('admissionChecks', []) if c['name'] == CHECK), {})
    match = re.match(r'^(?:Pending|Ready|Retry); attempt=([a-f0-9]+); reservation=([a-f0-9]+);', check.get('message', ''))
    return match[1] if match and match[2] == reservation_key(w) else None


class Bridge:
    def __init__(self, kubeconfig, namespace, trace, timeout=25):
        config.load_kube_config(config_file=kubeconfig, context='kind-scholarly-prototype')
        self.custom, self.core = client.CustomObjectsApi(), client.CoreV1Api()
        self.ns, self.trace, self.timeout = namespace, Path(trace), timeout
        self.trace.parent.mkdir(parents=True, exist_ok=True)

    def event(self, event, **data):
        with self.trace.open('a') as f:
            f.write(json.dumps(dict(time=datetime.now(timezone.utc).isoformat(),
                                    event=event, **data), sort_keys=True)+'\n')
            f.flush()

    def read(self, name):
        return self.custom.get_namespaced_custom_object(GROUP, VERSION, self.ns, 'workloads', name)

    def pods(self, w):
        owners = w['metadata'].get('ownerReferences', [])
        job = next((o for o in owners if o['kind'] == 'Job'), None)
        if not job:
            return []
        return self.core.list_namespaced_pod(self.ns, label_selector='batch.kubernetes.io/job-name='+job['name']).items

    def set_check(self, w, state, epoch, updates=None):
        body = copy.deepcopy(w)
        checks = body.setdefault('status', {}).setdefault('admissionChecks', [])
        check = next((c for c in checks if c['name'] == CHECK), None)
        if not check:
            return False
        check.update(state=state, lastTransitionTime=datetime.now(timezone.utc).isoformat(),
                     message=f'{state}; attempt={epoch}; reservation={reservation_key(w)}; placement follows admission')
        if updates is not None:
            check['podSetUpdates'] = updates
        try:
            # PUT /status includes the read resourceVersion: a stale write gets HTTP 409.
            self.custom.replace_namespaced_custom_object_status(GROUP, VERSION, self.ns,
                        'workloads', w['metadata']['name'], body)
            self.event('check_written', workload=w['metadata']['name'], state=state, attempt=epoch,
                       resource_version=w['metadata']['resourceVersion'])
            return True
        except ApiException as e:
            if e.status == 409:
                self.event('stale_write_rejected', workload=w['metadata']['name'], attempt=epoch,
                           resource_version=w['metadata']['resourceVersion'])
                return False
            raise

    def activate(self):
        obj = self.custom.get_cluster_custom_object(GROUP, VERSION, 'admissionchecks', CHECK)
        obj['status'] = {'conditions':[dict(type='Active', status='True', reason='ControllerRunning',
                    message='Single-process research bridge is running',
                    lastTransitionTime=datetime.now(timezone.utc).isoformat())]}
        self.custom.replace_cluster_custom_object_status(GROUP, VERSION, 'admissionchecks', CHECK, obj)

    def reconcile(self, w):
        name = w['metadata']['name']
        epoch = attempt(w)
        pods = self.pods(w)
        groups = self.custom.list_namespaced_custom_object('scheduling.volcano.sh','v1beta1',self.ns,
                  'podgroups',label_selector='prototype.example.com/workload-uid='+w['metadata']['uid'])['items']
        # Retain groups while pods drain; never clean up using just a quota counter.
        for pg in groups:
            if (not epoch or pg['metadata'].get('annotations',{}).get(ANNOTATION) != epoch) and not pods:
                self.custom.delete_namespaced_custom_object('scheduling.volcano.sh','v1beta1',self.ns,
                                                         'podgroups',pg['metadata']['name'])
                self.event('podgroup_deleted', workload=name, podgroup=pg['metadata']['name'])
        if not reservation_key(w) or condition(w,'Evicted') or condition(w,'Finished') or w.get('spec',{}).get('active') is False:
            return
        owners = w['metadata'].get('ownerReferences', [])
        if not any(o['kind']=='Job' and o['apiVersion']=='batch/v1' for o in owners):
            return
        check = next((c for c in w['status'].get('admissionChecks',[]) if c['name']==CHECK), None)
        if not check:
            return
        if not epoch:
            if check['state'] == 'Pending':
                self.set_check(w, 'Pending', uuid.uuid4().hex, [])
            return
        pgname = 'bridge-'+w['metadata']['uid'][:8]+'-'+epoch
        if check['state'] == 'Pending':
            sets = w['spec']['podSets']
            count = sum(s['count'] for s in sets)
            resources = {}
            for assignment in w['status']['admission']['podSetAssignments']:
                for resource, quantity in assignment.get('resourceUsage',{}).items():
                    if resource == 'example.com/accelerator':
                        resources[resource] = str(int(resources.get(resource,'0'))+int(quantity))
            pg = dict(apiVersion='scheduling.volcano.sh/v1beta1',kind='PodGroup',
                      metadata=dict(name=pgname,namespace=self.ns,
                        labels={'prototype.example.com/workload-uid':w['metadata']['uid']},
                        annotations={ANNOTATION:epoch},ownerReferences=[dict(apiVersion=w['apiVersion'],
                            kind='Workload',name=name,uid=w['metadata']['uid'])]),
                      spec=dict(minMember=count,minResources=resources,queue='prototype'))
            try:
                self.custom.create_namespaced_custom_object('scheduling.volcano.sh','v1beta1',self.ns,'podgroups',pg)
                self.event('podgroup_created',workload=name,podgroup=pgname,attempt=epoch)
            except ApiException as e:
                if e.status != 409:
                    raise
                existing = self.custom.get_namespaced_custom_object('scheduling.volcano.sh','v1beta1',self.ns,'podgroups',pgname)
                if existing['spec'] != pg['spec']:
                    raise ValueError('PodGroup contract changed')
            # Deliberate test pause after durable creation and before Ready, survives kill/restart.
            if w['metadata'].get('annotations',{}).get('prototype.example.com/pause-before-ready') == 'true':
                return
            fresh = self.read(name)
            fresh_check = next((c for c in fresh.get('status', {}).get('admissionChecks', [])
                                if c['name'] == CHECK), {})
            if (attempt(fresh) != epoch or condition(fresh,'Evicted') or condition(fresh,'Finished')
                    or fresh.get('spec', {}).get('active') is False or fresh_check.get('state') != 'Pending'):
                self.event('attempt_changed',workload=name,attempt=epoch)
                return
            updates = [dict(name=s['name'],annotations={'scheduling.k8s.io/group-name':pgname,ANNOTATION:epoch}) for s in sets]
            self.set_check(fresh,'Ready',epoch,updates)
        elif check['state'] == 'Ready' and condition(w,'Admitted'):
            admitted = datetime.fromisoformat(condition(w,'Admitted')['lastTransitionTime'].replace('Z','+00:00'))
            ready = len(pods) == sum(s['count'] for s in w['spec']['podSets']) and all(
                p.spec.node_name and p.status.phase == 'Running' for p in pods)
            if not ready and (datetime.now(timezone.utc)-admitted).total_seconds() > self.timeout:
                self.set_check(w,'Retry',epoch)

    def run(self):
        self.activate()
        self.event('controller_started')
        while True:
            objects = self.custom.list_namespaced_custom_object(GROUP,VERSION,self.ns,'workloads')['items']
            for w in objects:
                try:
                    self.reconcile(w)
                except ApiException as e:
                    if e.status != 404:
                        self.event('api_error',workload=w['metadata']['name'],status=e.status,reason=e.reason)
                except Exception as e:
                    self.event('controller_error',workload=w['metadata']['name'],error=str(e))
                    raise
            time.sleep(0.25)


if __name__ == '__main__':
    ap=argparse.ArgumentParser()
    ap.add_argument('--kubeconfig',required=True)
    ap.add_argument('--namespace',default='prototype')
    ap.add_argument('--trace',required=True)
    ap.add_argument('--timeout',type=int,default=25)
    a=ap.parse_args()
    Bridge(a.kubeconfig,a.namespace,a.trace,a.timeout).run()
