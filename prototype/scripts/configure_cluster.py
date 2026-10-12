"""Configure only the dedicated kind research cluster."""
from pathlib import Path
import json
import subprocess
import yaml
from kubernetes import client, config

BASE=Path(__file__).resolve().parents[1]
KUBE=BASE/'.runtime/kubeconfig'
config.load_kube_config(config_file=str(KUBE), context='kind-scholarly-prototype')
core=client.CoreV1Api()
objects=[{'apiVersion':'v1','kind':'Namespace','metadata':{'name':'prototype'}}]
objects.append(dict(apiVersion='kueue.x-k8s.io/v1beta1',kind='ResourceFlavor',
 metadata={'name':'synthetic-shared'},spec={'nodeLabels':{'prototype.example.com/pool':'shared'}}))
objects.append(dict(apiVersion='kueue.x-k8s.io/v1beta1',kind='AdmissionCheck',
 metadata={'name':'prototype-volcano'},spec={'controllerName':'prototype.example.com/volcano-bridge','retryDelayMinutes':1}))
objects.append(dict(apiVersion='kueue.x-k8s.io/v1beta1',kind='Cohort',metadata={'name':'prototype'}))
for name,priority,borrow,lend,reclaim in [('prod',100000,2,0,'Never'),('dev',1000,0,2,'Any')]:
 objects.append(dict(apiVersion='scheduling.k8s.io/v1',kind='PriorityClass',metadata={'name':'prototype-'+name},
  value=priority,preemptionPolicy='Never',globalDefault=False))
 objects.append(dict(apiVersion='kueue.x-k8s.io/v1beta1',kind='ClusterQueue',metadata={'name':'prototype-'+name},
  spec=dict(cohort='prototype',namespaceSelector={'matchLabels':{'kubernetes.io/metadata.name':'prototype'}},
   preemption={'withinClusterQueue':'Never','reclaimWithinCohort':reclaim,'borrowWithinCohort':{'policy':'Never'}},
   admissionChecks=['prototype-volcano'],resourceGroups=[{'coveredResources':['example.com/accelerator','cpu','memory'],
   'flavors':[{'name':'synthetic-shared','resources':[{'name':'example.com/accelerator','nominalQuota':2,
              'borrowingLimit':borrow,'lendingLimit':lend},
              {'name':'cpu','nominalQuota':'4','borrowingLimit':0,'lendingLimit':0},
              {'name':'memory','nominalQuota':'2Gi','borrowingLimit':0,'lendingLimit':0}]}]}])))
 objects.append(dict(apiVersion='kueue.x-k8s.io/v1beta1',kind='LocalQueue',
  metadata={'name':name,'namespace':'prototype'},spec={'clusterQueue':'prototype-'+name}))
objects.append(dict(apiVersion='scheduling.volcano.sh/v1beta1',kind='Queue',metadata={'name':'prototype'},
 spec={'weight':1,'reclaimable':False,'capability':{'example.com/accelerator':'4'}}))
objects.append(dict(apiVersion='v1',kind='ConfigMap',metadata={'name':'volcano-scheduler-configmap','namespace':'volcano-system'},
 data={'volcano-scheduler.conf':'''actions: "enqueue, allocate"
tiers:
- plugins:
  - name: priority
  - name: gang
    enablePreemptable: false
  - name: conformance
- plugins:
  - name: predicates
  - name: proportion
  - name: nodeorder
'''}))
path=BASE/'config/policies.yaml'
path.write_text(yaml.safe_dump_all(objects,sort_keys=False))
subprocess.run([str(BASE/'.tools/kubectl'),'--kubeconfig',str(KUBE),'apply','-f',str(path)],check=True)
for node in core.list_node().items:
 if 'control-plane' in node.metadata.name:
  continue
 core.patch_node(node.metadata.name,{'metadata':{'labels':{'prototype.example.com/pool':'shared'}}})
 core.patch_node_status(node.metadata.name,{'status':{'capacity':{'example.com/accelerator':'2'}}})
subprocess.run([str(BASE/'.tools/kubectl'),'--kubeconfig',str(KUBE),'rollout','restart','deployment/volcano-scheduler','-n','volcano-system'],check=True)
print('Configured 4 synthetic accelerator units across 2 workers; no physical GPUs.')
