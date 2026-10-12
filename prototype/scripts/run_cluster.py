"""Live-cluster correctness experiments, not simulated scheduler results."""
import argparse
import copy
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
import threading
from datetime import datetime, timezone
from kubernetes import client, config, watch

BASE=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(BASE/'src'))
from bridge import Bridge, condition, attempt


class Experiments:
 def __init__(self,out):
  self.out=out
  out.mkdir(parents=True,exist_ok=False)
  self.kube=str(BASE/'.runtime/kubeconfig')
  config.load_kube_config(config_file=self.kube,context='kind-scholarly-prototype')
  self.core,self.batch,self.custom=client.CoreV1Api(),client.BatchV1Api(),client.CustomObjectsApi()
  if (self.batch.list_namespaced_job('prototype').items or self.core.list_namespaced_pod('prototype').items
      or self.custom.list_namespaced_custom_object('kueue.x-k8s.io','v1beta1','prototype','workloads')['items']):
   raise RuntimeError('The prototype namespace must have no Jobs, Pods or Workloads before a run.')
  self.api=client.ApiClient()
  self.bridge=None
  self.log=None
  self.rows=[]
  self.release_observations=[]
  self.last=None
  self.case='setup'
  self.logic=Bridge(self.kube,'prototype',str(out/'bridge.jsonl'),timeout=20)
  self.history=[]
  self.watch_stop=threading.Event()
  self.watcher=threading.Thread(target=self.watch_workloads,daemon=True)
  self.watcher.start()

 def watch_workloads(self):
  api=client.CustomObjectsApi()
  rv=api.list_namespaced_custom_object('kueue.x-k8s.io','v1beta1','prototype','workloads')['metadata']['resourceVersion']
  while not self.watch_stop.is_set():
   try:
    for event in watch.Watch().stream(api.list_namespaced_custom_object,'kueue.x-k8s.io','v1beta1',
                  'prototype','workloads',resource_version=rv,timeout_seconds=3):
     obj=event['object'];rv=obj['metadata']['resourceVersion']
     self.history.append(obj)
     with (self.out/'workload-watch.jsonl').open('a') as f:
      f.write(json.dumps({'time':datetime.now(timezone.utc).isoformat(),'type':event['type'],'object':obj})+'\n')
   except Exception as error:
    with (self.out/'watch-errors.log').open('a') as f:f.write(str(error)+'\n')
    self.watch_stop.set()

 def historical(self,name,predicate):
  return next((w for w in reversed(self.history) if any(o['name']==name and o['kind']=='Job'
              for o in w['metadata'].get('ownerReferences',[])) and predicate(w)),None)

 def wl(self,job):
  objects=self.custom.list_namespaced_custom_object('kueue.x-k8s.io','v1beta1','prototype','workloads')['items']
  return next((w for w in objects if any(o['kind']=='Job' and o['name']==job for o in w['metadata'].get('ownerReferences',[]))),None)

 def pods(self,job):
  return self.core.list_namespaced_pod('prototype',label_selector='batch.kubernetes.io/job-name='+job).items

 def snapshot(self):
  snap={'case':self.case,'time':datetime.now(timezone.utc).isoformat(),
        'jobs':self.api.sanitize_for_serialization(self.batch.list_namespaced_job('prototype').items),
        'pods':self.api.sanitize_for_serialization(self.core.list_namespaced_pod('prototype').items)}
  for key,group,plural in [('workloads','kueue.x-k8s.io','workloads'),('podgroups','scheduling.volcano.sh','podgroups')]:
   snap[key]=self.custom.list_namespaced_custom_object(group,'v1beta1','prototype',plural)['items']
  snap['queues']=self.custom.list_cluster_custom_object('kueue.x-k8s.io','v1beta1','clusterqueues')['items']
  digest=hashlib.sha256(json.dumps({k:v for k,v in snap.items() if k!='time'},sort_keys=True).encode()).hexdigest()
  if digest!=self.last:
   with (self.out/'snapshots.jsonl').open('a') as f:f.write(json.dumps(snap,sort_keys=True)+'\n')
   self.last=digest
  if self.bridge and self.bridge.poll() is not None:
   raise RuntimeError('Bridge exited unexpectedly; inspect bridge-process.log')

 def wait(self,desc,fn,timeout=90):
  deadline=time.monotonic()+timeout
  while time.monotonic()<deadline:
   self.snapshot()
   value=fn()
   if value:return value
   time.sleep(.3)
  raise AssertionError('Timed out: '+desc)

 def observe(self,seconds):
  end=time.monotonic()+seconds
  while time.monotonic()<end:
   self.snapshot();time.sleep(.3)

 def start(self):
  self.log=(self.out/'bridge-process.log').open('a')
  self.bridge=subprocess.Popen([sys.executable,str(BASE/'src/bridge.py'),'--kubeconfig',self.kube,
     '--trace',str(self.out/'bridge.jsonl'),'--timeout','20'],stdout=self.log,stderr=subprocess.STDOUT)

 def stop(self,crash=False):
  if self.bridge:
   self.bridge.send_signal(signal.SIGKILL if crash else signal.SIGTERM)
   self.bridge.wait(timeout=10);self.bridge=None;self.log.close()

 def submit(self,name,queue='prod',impossible=False):
  template={'metadata':{'labels':{'prototype.example.com/job':name}},'spec':{
    'schedulerName':'volcano','priorityClassName':'prototype-'+queue,'restartPolicy':'Never',
    'terminationGracePeriodSeconds':5,'containers':[{'name':'worker','image':'busybox:1.37.0',
    'command':['sh','-c','trap "echo SIGTERM_RECEIVED; sleep 2; echo FLUSH_FINISHED; exit 0" TERM; echo WORKER_STARTED; while :; do sleep 1; done'],
    'resources':{'requests':{'cpu':'50m','memory':'32Mi','example.com/accelerator':'1'},
                 'limits':{'example.com/accelerator':'1'}}}]}}
  if impossible:template['spec']['nodeSelector']={'prototype.example.com/missing':'true'}
  job={'apiVersion':'batch/v1','kind':'Job','metadata':{'name':name,'namespace':'prototype',
    'labels':{'kueue.x-k8s.io/queue-name':queue}},'spec':{'suspend':True,'parallelism':2,'completions':2,
      'completionMode':'Indexed','backoffLimit':0,'template':template}}
  (self.out/(name+'-job.json')).write_text(json.dumps(job,indent=2))
  self.batch.create_namespaced_job('prototype',job)
  return self.wait('workload for '+name,lambda:self.wl(name))

 def running(self,name):
  w=self.wl(name);pods=self.pods(name)
  return w and condition(w,'Admitted') and len(pods)==2 and all(p.status.phase=='Running' for p in pods)

 def active(self,name,value):
  w=self.wl(name)
  self.custom.patch_namespaced_custom_object('kueue.x-k8s.io','v1beta1','prototype','workloads',w['metadata']['name'],{'spec':{'active':value}})

 def drain(self,name):
  self.active(name,False)
  self.wait('quota cleared '+name,lambda:not condition(self.wl(name),'QuotaReserved'))
  remaining=self.pods(name)
  observation=dict(job=name,time=datetime.now(timezone.utc).isoformat(),quota_reserved=False,
        remaining_pods=[dict(name=p.metadata.name,phase=p.status.phase,
              deleting=p.metadata.deletion_timestamp is not None) for p in remaining])
  self.release_observations.append(observation)
  (self.out/'quota-release-observations.json').write_text(json.dumps(self.release_observations,indent=2))
  self.wait('drained '+name,lambda:not self.pods(name) and not condition(self.wl(name),'QuotaReserved'))

 def clean(self):
  for job in self.batch.list_namespaced_job('prototype').items:
   if self.wl(job.metadata.name):self.drain(job.metadata.name)
   self.batch.delete_namespaced_job(job.metadata.name,'prototype',propagation_policy='Foreground')
  self.wait('all jobs gone',lambda:not self.batch.list_namespaced_job('prototype').items)
  self.wait('all workloads gone',lambda:not self.custom.list_namespaced_custom_object('kueue.x-k8s.io','v1beta1','prototype','workloads')['items'])

 def passed(self,case,**data):
  self.rows.append(dict(case=case,passed=True,**data))
  (self.out/'results.partial.json').write_text(json.dumps(self.rows,indent=2))
  print('PASS '+case,flush=True)

 def run(self):
  # Keep the bridge off to establish that reservation cannot create worker pods.
  self.case='reservation_before_admission'
  self.logic.activate()
  self.submit('normal')
  w=self.wait('quota held',lambda:(w if (w:=self.wl('normal')) and condition(w,'QuotaReserved') else None))
  assert not condition(w,'Admitted')
  assert self.batch.read_namespaced_job('normal','prototype').spec.suspend and not self.pods('normal')
  self.passed(self.case,quota_reserved=True,job_suspended=True,pod_count=0)
  self.start()
  self.wait('normal gang runs',lambda:self.running('normal'))
  w=self.wl('normal');epoch=attempt(w)
  for p in self.pods('normal'):
   assert p.spec.node_selector['prototype.example.com/pool']=='shared'
   assert self.core.read_node(p.spec.node_name).metadata.labels['prototype.example.com/pool']=='shared'
   assert p.metadata.annotations['prototype.example.com/attempt']==epoch
   assert p.metadata.annotations['scheduling.k8s.io/group-name'].endswith(epoch)
  self.passed('admission_flavor_and_group_propagation',workers=2,attempt=epoch)
  self.case='cancellation'
  self.drain('normal')
  self.passed(self.case,pods_remaining=0,quota_reserved=False)
  self.clean()

  self.case='restart_between_podgroup_and_ready'
  self.stop()
  self.submit('restart')
  w=self.wait('restart quota',lambda:(w if (w:=self.wl('restart')) and condition(w,'QuotaReserved') else None))
  self.custom.patch_namespaced_custom_object('kueue.x-k8s.io','v1beta1','prototype','workloads',w['metadata']['name'],
          {'metadata':{'annotations':{'prototype.example.com/pause-before-ready':'true'}}})
  self.start()
  pg=self.wait('PodGroup created before ready',lambda:next(iter(self.custom.list_namespaced_custom_object('scheduling.volcano.sh','v1beta1','prototype','podgroups')['items']),None))
  assert not self.pods('restart')
  self.stop(crash=True)
  self.custom.patch_namespaced_custom_object('kueue.x-k8s.io','v1beta1','prototype','workloads',w['metadata']['name'],
          {'metadata':{'annotations':{'prototype.example.com/pause-before-ready':None}}})
  self.start()
  self.wait('restart gang runs',lambda:self.running('restart'))
  groups=self.custom.list_namespaced_custom_object('scheduling.volcano.sh','v1beta1','prototype','podgroups')['items']
  assert len(groups)==1 and groups[0]['metadata']['uid']==pg['metadata']['uid']
  self.passed(self.case,podgroup_reused=True)

  self.case='stale_ready_after_readmission'
  stale=copy.deepcopy(self.wl('restart'));old_epoch=attempt(stale)
  self.stop()
  self.drain('restart')
  self.active('restart',True)
  fresh=self.wait('new quota attempt',lambda:(w if (w:=self.wl('restart')) and condition(w,'QuotaReserved') and attempt(w)!=old_epoch else None))
  assert not self.logic.set_check(stale,'Ready',old_epoch)
  self.start()
  self.wait('new attempt runs',lambda:self.running('restart'))
  assert attempt(self.wl('restart')) != old_epoch
  self.passed(self.case,old_attempt=old_epoch,new_attempt=attempt(self.wl('restart')),http_conflict=409)
  self.clean()

  self.case='placement_timeout_and_quota_release'
  self.submit('impossible',impossible=True)
  self.wait('impossible admitted',lambda:condition(self.wl('impossible'),'Admitted'))
  w=self.wait('retry evicts',lambda:self.historical('impossible',lambda w:condition(w,'Evicted')),timeout=60)
  self.wait('quota released after placement failure',lambda:self.historical('impossible',lambda w:
      not condition(w,'QuotaReserved') and any(c['type']=='Evicted' for c in w.get('status',{}).get('conditions',[]))))
  self.drain('impossible')
  self.passed(self.case,eviction=condition(w,'Evicted'),quota_released=True)
  self.clean()

  self.case='saturation_preserves_nominal'
  self.submit('prod-nominal','prod');self.wait('prod nominal runs',lambda:self.running('prod-nominal'))
  self.submit('dev-nominal','dev');self.wait('dev nominal runs',lambda:self.running('dev-nominal'))
  self.submit('prod-waits','prod');self.observe(6)
  assert not condition(self.wl('prod-waits'),'QuotaReserved') and not self.pods('prod-waits')
  assert self.running('prod-nominal') and self.running('dev-nominal')
  self.passed(self.case,prod_used=2,dev_used=2,new_prod_waits=True,observation_seconds=6)
  self.clean()

  self.case='borrowing_and_lender_reclaim'
  self.submit('prod-base','prod');self.wait('prod base runs',lambda:self.running('prod-base'))
  self.submit('prod-borrow','prod');self.wait('prod borrower runs',lambda:self.running('prod-borrow'))
  self.passed('borrow_idle_quota',prod_used=4,prod_nominal=2,dev_used=0)
  self.submit('dev-reclaim','dev')
  self.wait('dev reclaims',lambda:self.running('dev-reclaim'))
  preempted=[name for name in ('prod-base','prod-borrow') if condition(self.wl(name),'Preempted')]
  assert preempted
  self.wait('borrower drained',lambda:all(not self.pods(name) for name in preempted))
  assert len(preempted)==1
  assert self.running(next(name for name in ('prod-base','prod-borrow') if name not in preempted))
  self.passed(self.case,preempted=preempted,dev_priority=1000,prod_priority=100000,
              explanation='Dev reclaims its own nominal quota using Any; priority does not override nominal protection')
  self.clean()

  events=self.core.list_namespaced_event('prototype')
  (self.out/'kubernetes-events.json').write_text(json.dumps(self.api.sanitize_for_serialization(events),indent=2))
  self.stop()
  assert not (self.out/'watch-errors.log').exists(), 'Incomplete watch trace'
  (self.out/'results.json').write_text(json.dumps(dict(created_utc=datetime.now(timezone.utc).isoformat(),
     kubernetes=client.VersionApi().get_code().git_version,kueue='v0.17.0',volcano='v1.13.0',
     synthetic_accelerators=4,physical_gpus=0,cases=self.rows,
     quota_release_observations=self.release_observations),indent=2))


if __name__=='__main__':
 ap=argparse.ArgumentParser();ap.add_argument('--output',required=True);a=ap.parse_args()
 e=Experiments(Path(a.output).resolve())
 try:e.run()
 finally:
  e.stop()
  e.watch_stop.set()
  e.watcher.join(timeout=5)
