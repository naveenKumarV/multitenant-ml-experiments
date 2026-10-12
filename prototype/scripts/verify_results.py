"""Check retained machine-readable findings without rerunning the experiments."""
from pathlib import Path
import json

root = Path(__file__).resolve().parents[1]
expected_cluster = {
    'reservation_before_admission', 'admission_flavor_and_group_propagation',
    'cancellation', 'restart_between_podgroup_and_ready', 'stale_ready_after_readmission',
    'placement_timeout_and_quota_release', 'saturation_preserves_nominal',
    'borrow_idle_quota', 'borrowing_and_lender_reclaim',
}
for run in ('cluster-final', 'cluster-reproduced'):
    directory = root / 'results' / run
    data = json.loads((directory / 'results.json').read_text())
    assert len(data['cases']) == 9
    assert {c['case'] for c in data['cases']} == expected_cluster
    assert all(c['passed'] for c in data['cases'])
    assert not (directory / 'watch-errors.log').exists()
    print(f'{run}: all nine checks passed; API watch reported no errors')

data = json.loads((root / 'results/checkpoint-final/results.json').read_text())
positive = [c for c in data['cases'] if 'resume_step' in c]
negative = [c for c in data['cases'] if 'expected_divergence' in c]
assert len(positive) == 18 and len(negative) == 9
assert {c['seed'] for c in data['cases']} == {7, 19, 41}
expected_steps = {'sigterm': 35, 'abrupt': 20, 'missing_rank': 20, 'torn_write': 20,
                  'coordinator_before_manifest': 20, 'coordinator_after_manifest': 35}
for seed in (7, 19, 41):
    rows = [c for c in positive if c['seed'] == seed]
    assert len(rows) == 6 and {c['scenario'] for c in rows} == set(expected_steps)
    for c in rows:
        assert c['pointer_valid'] and c['continuation_trace_equal']
        assert len(c['final_state_equal']) == 14 and all(c['final_state_equal'].values())
        assert c['resume_step'] == expected_steps[c['scenario']]
        assert c['lost_steps'] == 35 - c['resume_step']
    controls = [c for c in negative if c['seed'] == seed]
    assert {c['scenario'] for c in controls} == {'omit_rng', 'omit_data', 'omit_optimizer'}
    assert all(c['expected_divergence'] and not c['model_equal'] for c in controls)
print('checkpoint-final: 18 exact recoveries and nine expected negative-control divergences')
print('This validates summary consistency; raw execution evidence remains in results/.')
