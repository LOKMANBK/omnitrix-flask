"""
ALNS (Adaptive Large Neighborhood Search) Engine
Hazır Beton Sevkiyat Optimizasyonu — Limak Çimento Dijital İkiz

JavaScript'ten Python'a bire-bir port edildi.
Tüm MIP kısıtları, maliyet fonksiyonu ve operatörler korunmuştur.
"""

import math
import time
import copy
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Callable, Any

# ─── Sabitler ───
BATCH_DUR = 8
UNLOAD_DUR = 8
WASH_DUR = 10
CONCRETE_LIMIT = 60
SHIFT_START = 420
SHIFT_END = 1440
PUMP_SETUP = 15
PUMP_TEARDOWN = 15


# ─── Maliyet Konfigürasyonu ───
@dataclass
class CostConfig:
    makespan_weight: float = 2.5
    truck_activation_cost: float = 50
    fuel_cost_per_km: float = 0.5
    wait_weight: float = 8
    pump_idle_weight: float = 7
    pump_transfer_weight: float = 5
    inter_site_multiplier: float = 5
    unserved_penalty: float = 200


COST_CFG = CostConfig()


# ─── Seeded RNG ───
class SeededRNG:
    def __init__(self, seed: int = 42):
        self._seed = seed

    def next(self) -> float:
        self._seed = (self._seed * 9301 + 49297) % 233280
        return self._seed / 233280

    def rand_int(self, a: int, b: int) -> int:
        return a + int(math.floor(self.next() * (b - a + 1)))

    def shuffle(self, arr: list) -> list:
        for i in range(len(arr) - 1, 0, -1):
            j = int(math.floor(self.next() * (i + 1)))
            arr[i], arr[j] = arr[j], arr[i]
        return arr

    def sample(self, arr: list, n: int) -> list:
        c = arr[:]
        self.shuffle(c)
        return c[:min(n, len(c))]


# ─── Haversine mesafe (km) ───
def haversine(lat1, lng1, lat2, lng2):
    R = 6371
    to_r = math.pi / 180
    d_lat = (lat2 - lat1) * to_r
    d_lng = (lng2 - lng1) * to_r
    a = math.sin(d_lat / 2) ** 2 + math.cos(lat1 * to_r) * math.cos(lat2 * to_r) * math.sin(d_lng / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


# ─── Trafik çarpanı (deterministik, plant+customer+saat bazlı hash) ───
_traffic_cache: Dict[str, float] = {}


def traffic_mult(pid: str, cid: str, minute_of_day: float) -> float:
    hour_block = int(minute_of_day // 180)
    key = f"{pid}_{cid}_{hour_block}"
    if key in _traffic_cache:
        return _traffic_cache[key]

    h = 0
    for ch in key:
        h = ((h << 5) - h) + ord(ch)
        h &= 0xFFFFFFFF  # 32-bit
    pseudo = abs(math.sin(h * 9301 + 49297)) % 1

    if minute_of_day >= 1200:
        mult = 1.0
    elif minute_of_day >= 1080:
        mult = 1.0 + pseudo * 0.1
    elif minute_of_day >= 900:
        mult = 1.1 + pseudo * 0.2
    elif minute_of_day >= 600:
        mult = 1.15 + pseudo * 0.2
    else:
        mult = 1.05 + pseudo * 0.1

    _traffic_cache[key] = round(mult * 100) / 100
    return _traffic_cache[key]


# ─── Problem yapısı ───
class Problem:
    def __init__(self, data: dict, orig_travel_base: dict = None):
        self.plants: Dict[str, dict] = {}
        self.trucks: Dict[str, dict] = {}
        self.customers: Dict[str, dict] = {}
        self.pumps: Dict[str, dict] = {}
        self._dist_cache: Dict[str, float] = {}
        self._orig_travel_base = orig_travel_base or {}
        self.trucks_by_plant: Dict[str, List[str]] = {}

        for p in data.get('plants', []):
            self.plants[p['id']] = dict(p)

        for t in data.get('trucks', []):
            td = dict(t)
            td['capacity_m3'] = td.get('capacity_m3', td.get('capacity', 9))
            self.trucks[t['id']] = td

        for p in data.get('pumps', []):
            self.pumps[p['id']] = dict(p)

        for c in data.get('customers', []):
            cd = dict(c)
            irsaliye = cd.get('irsaliye', '07:00:00-23:59:00')
            tw = irsaliye.split('-')

            def parse_t(s):
                if not s:
                    return 420
                parts = s.split(':')
                try:
                    m = int(parts[0]) * 60 + int(parts[1] if len(parts) > 1 else 0)
                except (ValueError, IndexError):
                    return 420
                return m

            tw_s = parse_t(tw[0] if tw else None)
            tw_e = parse_t(tw[1] if len(tw) > 1 else None)

            req_raw = cd.get('does_it_require_mobile_pump', '')
            needs_pump = req_raw in ('evet', 'yes', True, 1, '1', 'true')

            cd['irsaliye'] = irsaliye
            cd['tw_start'] = tw_s
            cd['tw_end'] = tw_e if tw_e > tw_s else 1440
            cd['needs_pump'] = needs_pump
            self.customers[c['id']] = cd

        # Araçları tesislere göre grupla
        for tid, t in self.trucks.items():
            pid = t.get('current_plant_id', '')
            if pid not in self.trucks_by_plant:
                self.trucks_by_plant[pid] = []
            self.trucks_by_plant[pid].append(tid)

    def dist_km(self, pid: str, cid: str) -> float:
        k = f"{pid}_{cid}"
        if k not in self._dist_cache:
            p = self.plants[pid]
            c = self.customers[cid]
            self._dist_cache[k] = haversine(p['lat'], p['lng'], c['lat'], c['lng'])
        return self._dist_cache[k]

    def travel_time(self, pid: str, cid: str, mult: float = 1.0) -> float:
        k = f"{pid}_{cid}"
        if k in self._orig_travel_base:
            return self._orig_travel_base[k] * mult
        d = self.dist_km(pid, cid)
        base = max(5, (d / 40) * 60)
        return round(base) * mult


# ─── Trip yapısı ───
@dataclass
class Trip:
    trip_idx: int
    tid: str
    pid: str
    cid: str
    amount: float
    batch_start: float
    batch_end: float
    arrive: float
    unload_start: float
    unload_end: float
    wash_end: float
    leave_site: float
    return_plant: float
    wait: float
    t_mult_to: float
    t_mult_back: float
    concrete_age: float
    travel_to: float
    travel_back_dur: float
    pump_id: str = ''


# ─── Çözüm sınıfı ───
class Solution:
    def __init__(self, prob: Problem):
        self.prob = prob
        self.routes: Dict[str, dict] = {}
        self.removed: List[dict] = []

        for cid, c in prob.customers.items():
            self.routes[cid] = {
                'cid': cid,
                'pid': '',
                'trips': [],
                'total_demand': c.get('total_demand', 0),
                'needs_pump': c.get('needs_pump', False),
                'tw_start': c.get('tw_start', 420),
                'tw_end': c.get('tw_end', 1440),
            }

    def truck_free(self, tid: str) -> float:
        latest = SHIFT_START
        for r in self.routes.values():
            for t in r['trips']:
                if t.tid == tid:
                    latest = max(latest, t.return_plant)
        return latest

    def plant_slot(self, pid: str, earliest: float) -> float:
        cap = self.prob.plants[pid].get('batching_stations', 2)
        ends = []
        for r in self.routes.values():
            for t in r['trips']:
                if t.pid == pid:
                    ends.append(t.batch_end)
        ends.sort()
        if len(ends) >= cap:
            return max(earliest, ends[len(ends) - cap])
        return earliest

    def site_unload_free_time(self, cid: str) -> float:
        r = self.routes.get(cid)
        if not r or not r['trips']:
            return 0
        return max(t.unload_end for t in r['trips'])

    def schedule_trip(self, tid, pid, cid, amount, trip_idx, rng,
                      pump_ready=0, pump_id='', is_last=True,
                      already_on_site=False) -> Optional[Trip]:
        c = self.prob.customers[cid]
        truck = self.prob.trucks.get(tid)
        if not truck:
            return None

        # is_last_trip kısıtı (MIP kısıt 24)
        if not is_last and abs(amount - truck['capacity_m3']) > 0.01:
            return None

        t_free = self.truck_free(tid)
        t_mult_est = traffic_mult(pid, cid, max(t_free, SHIFT_START))
        travel_est = self.prob.travel_time(pid, cid, t_mult_est)

        if already_on_site:
            target_arrival = pump_ready
        elif c['needs_pump'] and pump_ready > SHIFT_START:
            target_arrival = max(c['tw_start'], pump_ready + PUMP_SETUP)
        else:
            target_arrival = c['tw_start']

        earliest_batch = max(t_free, SHIFT_START, target_arrival - travel_est - BATCH_DUR)
        earliest_batch = max(earliest_batch, self.plant_slot(pid, earliest_batch))

        batch_start = earliest_batch
        batch_end = batch_start + BATCH_DUR

        t_mult = traffic_mult(pid, cid, batch_end)
        travel = self.prob.travel_time(pid, cid, t_mult)
        arrive = batch_end + travel

        if arrive > c['tw_end']:
            return None

        site_free = self.site_unload_free_time(cid)
        if c['needs_pump']:
            pump_setup_done = pump_ready if already_on_site else (pump_ready + PUMP_SETUP)
            unload_start = max(arrive, c['tw_start'], pump_setup_done, site_free)
        else:
            unload_start = max(arrive, c['tw_start'], site_free)

        wait = max(0, unload_start - arrive)
        unload_end = unload_start + UNLOAD_DUR

        if unload_end > c['tw_end']:
            return None

        concrete_age = unload_start - batch_end
        if concrete_age > CONCRETE_LIMIT:
            return None

        wash_end = unload_end + WASH_DUR
        t_mult_back = traffic_mult(pid, cid, wash_end)
        travel_back = self.prob.travel_time(pid, cid, t_mult_back)
        return_plant = wash_end + travel_back

        if return_plant > SHIFT_END + 60:
            return None

        return Trip(
            trip_idx=trip_idx, tid=tid, pid=pid, cid=cid, amount=amount,
            batch_start=batch_start, batch_end=batch_end, arrive=arrive,
            unload_start=unload_start, unload_end=unload_end, wash_end=wash_end,
            leave_site=wash_end, return_plant=return_plant, wait=wait,
            t_mult_to=t_mult, t_mult_back=t_mult_back, concrete_age=concrete_age,
            travel_to=travel, travel_back_dur=travel_back, pump_id=pump_id
        )

    def objective(self) -> float:
        cost = 0.0
        used_trucks = set()
        pump_jobs: Dict[str, list] = {}

        for cid, r in self.routes.items():
            delivered = sum(t.amount for t in r['trips'])
            unserved = r['total_demand'] - delivered
            if unserved > 0:
                cost += unserved * COST_CFG.unserved_penalty

            for t in r['trips']:
                cost += max(0, t.concrete_age - CONCRETE_LIMIT) * 50
                cost += t.wait * COST_CFG.wait_weight
                km = self.prob.dist_km(t.pid, t.cid) * 2
                cost += km * COST_CFG.fuel_cost_per_km
                used_trucks.add(t.tid)

                if t.pump_id:
                    if t.pump_id not in pump_jobs:
                        pump_jobs[t.pump_id] = []
                    pump_jobs[t.pump_id].append({
                        'unload_start': t.unload_start,
                        'unload_end': t.unload_end
                    })

            # Pompa aynı şantiye araçlar arası boşta kalma
            if r['needs_pump'] and len(r['trips']) > 1:
                sorted_trips = sorted(r['trips'], key=lambda x: x.unload_start)
                for i in range(1, len(sorted_trips)):
                    pump_idle = max(0, sorted_trips[i].unload_start - sorted_trips[i - 1].unload_end)
                    cost += pump_idle * COST_CFG.pump_idle_weight

        for rm in self.removed:
            cost += rm['amount'] * COST_CFG.unserved_penalty

        # Pompa şantiyeler arası transfer boşluğu
        for pid, jobs in pump_jobs.items():
            jobs.sort(key=lambda x: x['unload_start'])
            for i in range(1, len(jobs)):
                prev_teardown_end = jobs[i - 1]['unload_end'] + PUMP_TEARDOWN
                next_setup_start = jobs[i]['unload_start'] - PUMP_SETUP
                transfer_gap = max(0, next_setup_start - prev_teardown_end)
                cost += transfer_gap * COST_CFG.pump_transfer_weight

        cost += len(used_trucks) * COST_CFG.truck_activation_cost

        makespan = SHIFT_START
        for r in self.routes.values():
            for t in r['trips']:
                makespan = max(makespan, t.return_plant)
        cost += (makespan - SHIFT_START) * COST_CFG.makespan_weight

        return cost

    def stats(self) -> dict:
        all_trips = []
        for r in self.routes.values():
            all_trips.extend(r['trips'])
        total_m3 = sum(t.amount for t in all_trips)
        total_demand = sum(r['total_demand'] for r in self.routes.values())
        risk = sum(1 for t in all_trips if t.concrete_age >= 60)
        avg_age = (sum(t.concrete_age for t in all_trips) / len(all_trips)) if all_trips else 0
        total_wait = sum(t.wait for t in all_trips)
        return {
            'trips': len(all_trips),
            'm3': round(total_m3 * 10) / 10,
            'demand': total_demand,
            'pct': round(1000 * total_m3 / total_demand) / 10 if total_demand else 0,
            'risk': risk,
            'avgAge': round(avg_age * 10) / 10,
            'wait': round(total_wait * 10) / 10,
            'obj': round(self.objective() * 100) / 100,
        }

    def copy(self) -> 'Solution':
        s = Solution(self.prob)
        for cid in self.routes:
            s.routes[cid]['pid'] = self.routes[cid]['pid']
            s.routes[cid]['trips'] = [
                Trip(**{f.name: getattr(t, f.name) for f in t.__dataclass_fields__.values()})
                for t in self.routes[cid]['trips']
            ]
        for r in self.removed:
            s.removed.append(dict(r))
        return s


# ─── Pompa adayı yardımcısı ───
def pump_candidates(sol: Solution, cid: str) -> list:
    c = sol.prob.customers[cid]
    if not c['needs_pump']:
        return [{'pump_id': '', 'pump_ready': 0, 'already_on_site': False, 'from_site': False, 'last_cid': None}]

    pumps = list(sol.prob.pumps.values())
    if not pumps:
        return []

    req_type = c.get('required_mobile_pump_type')
    req_cap = c.get('required_min_pump_capacity', 0) or 0

    eligible = [p for p in pumps
                if (not req_type or p.get('mobile_pump_type') == req_type)
                and (not req_cap or (p.get('pump_capacity', 0) or 0) >= req_cap)]

    if not eligible:
        return []

    results = []
    site_trips = sol.routes.get(cid, {}).get('trips', [])

    for p in eligible:
        same_trips = [t for t in site_trips if t.pump_id == p['id']]
        if same_trips:
            last_unload_end = max(t.unload_end for t in same_trips)
            results.append({
                'pump_id': p['id'], 'pump_ready': last_unload_end,
                'already_on_site': True, 'from_site': False, 'last_cid': None
            })
            continue

        free_at = SHIFT_START
        last_cid_val = None
        for cid2, r2 in sol.routes.items():
            if cid2 == cid:
                continue
            for t in r2['trips']:
                if t.pump_id == p['id']:
                    end_time = t.unload_end + PUMP_TEARDOWN
                    if end_time > free_at:
                        free_at = end_time
                        last_cid_val = cid2

        if last_cid_val is not None:
            prev_cust = sol.prob.customers.get(last_cid_val)
            new_cust = sol.prob.customers.get(cid)
            if prev_cust and new_cust:
                d_km = haversine(prev_cust['lat'], prev_cust['lng'],
                                 new_cust['lat'], new_cust['lng'])
                tt = max(5, round((d_km / 40) * 60 * 1.2))
            else:
                k = f"{p['plant_id']}_{cid}"
                tt = sol.prob._orig_travel_base.get(k, sol.prob.travel_time(p['plant_id'], cid, 1.2)) or 15
        else:
            k = f"{p['plant_id']}_{cid}"
            tt = sol.prob._orig_travel_base.get(k, sol.prob.travel_time(p['plant_id'], cid, 1.2)) or 15

        pump_ready = free_at + tt if free_at > SHIFT_START else SHIFT_START + tt
        from_site = last_cid_val is not None
        results.append({
            'pump_id': p['id'], 'pump_ready': pump_ready,
            'already_on_site': False, 'from_site': from_site, 'last_cid': last_cid_val
        })

    return results


# ─── Greedy başlangıç çözümü ───
def build_greedy(prob: Problem, rng: SeededRNG) -> Solution:
    sol = Solution(prob)
    sorted_customers = sorted(prob.customers.values(), key=lambda c: c['tw_start'])

    for cust in sorted_customers:
        remaining = cust['total_demand']
        trip_idx = 0
        plants_by_dist = sorted(prob.plants.keys(),
                                key=lambda pid: prob.travel_time(pid, cust['id'], 1.0))

        while remaining > 0.5:
            p_cands = pump_candidates(sol, cust['id'])
            if not p_cands:
                sol.routes[cust['id']]['removed'] = True
                sol.removed.append({'cid': cust['id'], 'amount': remaining})
                break

            best_trip = None
            best_score = float('inf')
            best_pid = ''

            for pid in plants_by_dist:
                t_list = prob.trucks_by_plant.get(pid, [])
                sorted_trucks = sorted(t_list, key=lambda tid: sol.truck_free(tid))

                for tid in sorted_trucks:
                    cap = prob.trucks[tid].get('capacity_m3', 12)
                    amount = min(remaining, cap)
                    is_last = (remaining - amount) < 0.5

                    for pc in p_cands:
                        trip = sol.schedule_trip(
                            tid, pid, cust['id'], amount, trip_idx, rng,
                            pc['pump_ready'], pc['pump_id'], is_last,
                            pc['already_on_site']
                        )
                        if not trip:
                            continue

                        pump_idle_g = 0
                        if cust['needs_pump']:
                            prev_pt = [t for t in sol.routes[cust['id']]['trips']
                                        if t.pump_id == trip.pump_id]
                            if prev_pt:
                                last_e = max(t.unload_end for t in prev_pt)
                                pump_idle_g = max(0, trip.unload_start - last_e)

                        inter_site_penalty = 0
                        if pc.get('from_site') and pc.get('last_cid'):
                            prev_c = prob.customers.get(pc['last_cid'])
                            new_c = prob.customers.get(cust['id'])
                            if prev_c and new_c:
                                d_km = haversine(prev_c['lat'], prev_c['lng'],
                                                 new_c['lat'], new_c['lng'])
                                inter_site_penalty = max(5, round((d_km / 40) * 60 * 1.2)) * COST_CFG.inter_site_multiplier
                            else:
                                inter_site_penalty = 60 * COST_CFG.inter_site_multiplier
                        elif pc.get('from_site'):
                            inter_site_penalty = 60 * COST_CFG.inter_site_multiplier

                        score = (trip.batch_start + trip.concrete_age * 0.5
                                 + pump_idle_g * COST_CFG.pump_idle_weight
                                 + inter_site_penalty)
                        if score < best_score:
                            best_score = score
                            best_trip = trip
                            best_pid = pid

            if not best_trip:
                sol.removed.append({'cid': cust['id'], 'amount': remaining})
                break

            sol.routes[cust['id']]['pid'] = best_pid
            sol.routes[cust['id']]['trips'].append(best_trip)
            remaining -= best_trip.amount
            trip_idx += 1

    return sol


# ─── Mevcut D çözümünü ALNS formatına aktar ───
def import_current_solution(prob: Problem, rng: SeededRNG, data: dict) -> Solution:
    sol = Solution(prob)
    for r in data.get('solution', {}).get('routes', []):
        cid = r['customer_id']
        if cid not in sol.routes:
            continue
        sol.routes[cid]['pid'] = r.get('plant_id', '')
        for t in r.get('trips', []):
            c = prob.customers.get(cid)
            if not c:
                continue
            t_mult_to = t.get('traffic_multiplier_to', 1.2)
            t_mult_back = t.get('traffic_multiplier_back', 1.2)
            travel_to = t.get('travel_to_duration') or prob.travel_time(t['plant_id'], cid, t_mult_to)
            travel_back_dur = t.get('travel_back_duration') or travel_to
            wait_dur = t.get('wait_duration', 0)
            concrete_age = t.get('unload_start', 0) - (t.get('batch_end') or (t.get('batch_start', 0) + 8))

            sol.routes[cid]['trips'].append(Trip(
                trip_idx=t.get('trip_index', 0), tid=t['truck_id'],
                pid=t['plant_id'], cid=cid, amount=t['amount'],
                batch_start=t['batch_start'], batch_end=t.get('batch_end', t['batch_start'] + 8),
                arrive=t['arrive_site'], unload_start=t['unload_start'],
                unload_end=t['unload_end'],
                wash_end=t.get('wash_end', t['unload_end'] + WASH_DUR),
                leave_site=t.get('leave_site', t.get('wash_end', t['unload_end'] + WASH_DUR)),
                return_plant=t['return_plant'], wait=wait_dur,
                t_mult_to=t_mult_to, t_mult_back=t_mult_back,
                concrete_age=concrete_age, travel_to=travel_to,
                travel_back_dur=travel_back_dur,
                pump_id=t.get('pump_id', '')
            ))
    return sol


# ═══════════════════════════════════════════════
#  DESTROY OPERATÖRLERİ
# ═══════════════════════════════════════════════

def random_removal(sol: Solution, n_remove: int, rng: SeededRNG) -> Solution:
    nw = sol.copy()
    all_items = []
    for cid, r in nw.routes.items():
        for i in range(len(r['trips'])):
            all_items.append((cid, i))
    if not all_items:
        return nw
    to_rm = rng.sample(all_items, n_remove)
    by_route: Dict[str, list] = {}
    for cid, i in to_rm:
        by_route.setdefault(cid, []).append(i)
    for cid, indices in by_route.items():
        for i in sorted(indices, reverse=True):
            t = nw.routes[cid]['trips'].pop(i)
            nw.removed.append({'cid': cid, 'amount': t.amount})
    return nw


def worst_cost_removal(sol: Solution, n_remove: int, rng: SeededRNG) -> Solution:
    nw = sol.copy()
    scored = []
    for cid, r in nw.routes.items():
        for i, t in enumerate(r['trips']):
            scored.append({
                'cost': (t.concrete_age * 0.6 + t.wait * 0.4) * (0.9 + rng.next() * 0.2),
                'cid': cid, 'i': i
            })
    scored.sort(key=lambda x: x['cost'], reverse=True)
    by_route: Dict[str, list] = {}
    for item in scored[:n_remove]:
        by_route.setdefault(item['cid'], []).append(item['i'])
    for cid, indices in by_route.items():
        for i in sorted(indices, reverse=True):
            t = nw.routes[cid]['trips'].pop(i)
            nw.removed.append({'cid': cid, 'amount': t.amount})
    return nw


def related_removal(sol: Solution, n_remove: int, rng: SeededRNG) -> Solution:
    nw = sol.copy()
    all_items = []
    for cid, r in nw.routes.items():
        for i in range(len(r['trips'])):
            all_items.append((cid, i))
    if not all_items:
        return nw
    seed_idx = int(rng.next() * len(all_items))
    seed_cid, seed_i = all_items[seed_idx]
    seed_trip = nw.routes[seed_cid]['trips'][seed_i]
    ranked = []
    for cid, i in all_items:
        t = nw.routes[cid]['trips'][i]
        sc = 0
        if t.pid == seed_trip.pid:
            sc += 3
        if t.tid == seed_trip.tid:
            sc += 4
        sc += max(0, 2 - abs(t.batch_start - seed_trip.batch_start) / 60)
        ranked.append({'cid': cid, 'i': i, 'sc': sc})
    ranked.sort(key=lambda x: x['sc'], reverse=True)
    by_route: Dict[str, list] = {}
    for item in ranked[:n_remove]:
        by_route.setdefault(item['cid'], []).append(item['i'])
    for cid, indices in by_route.items():
        for i in sorted(indices, reverse=True):
            t = nw.routes[cid]['trips'].pop(i)
            nw.removed.append({'cid': cid, 'amount': t.amount})
    return nw


def time_window_removal(sol: Solution, n_remove: int, rng: SeededRNG) -> Solution:
    nw = sol.copy()
    risky, safe = [], []
    for cid, r in nw.routes.items():
        for i, t in enumerate(r['trips']):
            target = risky if t.concrete_age >= 60 else safe
            target.append({'age': t.concrete_age, 'cid': cid, 'i': i})
    risky.sort(key=lambda x: x['age'], reverse=True)
    candidates = risky + safe
    by_route: Dict[str, list] = {}
    for item in candidates[:n_remove]:
        by_route.setdefault(item['cid'], []).append(item['i'])
    for cid, indices in by_route.items():
        for i in sorted(indices, reverse=True):
            t = nw.routes[cid]['trips'].pop(i)
            nw.removed.append({'cid': cid, 'amount': t.amount})
    return nw


def truck_chain_removal(sol: Solution, n_remove: int, rng: SeededRNG) -> Solution:
    nw = sol.copy()
    by_truck: Dict[str, list] = {}
    for cid, r in nw.routes.items():
        for i, t in enumerate(r['trips']):
            by_truck.setdefault(t.tid, []).append({'cid': cid, 'i': i})
    entries = list(by_truck.items())
    if not entries:
        return nw
    entries.sort(key=lambda x: abs(len(x[1]) - n_remove))
    chosen = entries[0][1]
    by_route: Dict[str, list] = {}
    for item in chosen:
        by_route.setdefault(item['cid'], []).append(item['i'])
    for cid, indices in by_route.items():
        for i in sorted(indices, reverse=True):
            t = nw.routes[cid]['trips'].pop(i)
            nw.removed.append({'cid': cid, 'amount': t.amount})
    return nw


# ═══════════════════════════════════════════════
#  REPAIR YARDIMCISI
# ═══════════════════════════════════════════════

def best_insertion(sol: Solution, cid: str, amount: float, rng: SeededRNG,
                   randomize: bool = False, is_last: bool = True) -> Optional[dict]:
    route = sol.routes.get(cid)
    if not route:
        return None
    trip_idx = len(route['trips'])
    p_cands = pump_candidates(sol, cid)
    if not p_cands:
        return None
    candidates = []
    for pid in sol.prob.plants:
        t_list = sol.prob.trucks_by_plant.get(pid, [])
        for tid in t_list:
            for pc in p_cands:
                trip = sol.schedule_trip(
                    tid, pid, cid, amount, trip_idx, rng,
                    pc['pump_ready'], pc['pump_id'], is_last,
                    pc['already_on_site']
                )
                if not trip:
                    continue

                pump_idle_gap = 0
                if route['needs_pump']:
                    prev_pump_trips = [t for t in route['trips'] if t.pump_id == trip.pump_id]
                    if prev_pump_trips:
                        last_end = max(t.unload_end for t in prev_pump_trips)
                        pump_idle_gap = max(0, trip.unload_start - last_end)

                inter_site_penalty = 0
                if pc.get('from_site') and pc.get('last_cid'):
                    prev_c = sol.prob.customers.get(pc['last_cid'])
                    new_c = sol.prob.customers.get(cid)
                    if prev_c and new_c:
                        d_km = haversine(prev_c['lat'], prev_c['lng'], new_c['lat'], new_c['lng'])
                        inter_site_penalty = max(5, round((d_km / 40) * 60 * 1.2)) * COST_CFG.inter_site_multiplier
                    else:
                        inter_site_penalty = 60 * COST_CFG.inter_site_multiplier
                elif pc.get('from_site'):
                    inter_site_penalty = 60 * COST_CFG.inter_site_multiplier

                score = (trip.batch_start + trip.concrete_age * 0.5 + trip.wait
                         + pump_idle_gap * COST_CFG.pump_idle_weight + inter_site_penalty)
                candidates.append({'score': score, 'trip': trip, 'pid': pid, 'tid': tid})

    if not candidates:
        return None
    if randomize:
        rng.shuffle(candidates)
        return candidates[0]
    candidates.sort(key=lambda x: x['score'])
    return candidates[0]


# ═══════════════════════════════════════════════
#  REPAIR OPERATÖRLERİ
# ═══════════════════════════════════════════════

def greedy_repair(sol: Solution, rng: SeededRNG) -> Solution:
    nw = sol.copy()
    pending = sorted(nw.removed[:], key=lambda x: sol.prob.customers[x['cid']]['tw_start'])
    nw.removed.clear()
    for item in pending:
        cid, amount = item['cid'], item['amount']
        delivered = sum(t.amount for t in nw.routes[cid]['trips'])
        is_last = (delivered + amount) >= nw.routes[cid]['total_demand'] - 0.5
        res = best_insertion(nw, cid, amount, rng, False, is_last)
        if res:
            nw.routes[cid]['trips'].append(res['trip'])
            nw.routes[cid]['pid'] = res['pid']
        else:
            nw.removed.append({'cid': cid, 'amount': amount})
    return nw


def regret2_repair(sol: Solution, rng: SeededRNG) -> Solution:
    nw = sol.copy()
    pending = nw.removed[:]
    nw.removed.clear()
    while pending:
        best_regret = float('-inf')
        best_idx = 0
        best_trip_obj = None
        best_pid = ''
        for i, item in enumerate(pending):
            cid, amount = item['cid'], item['amount']
            p_cands = pump_candidates(nw, cid)
            if not p_cands:
                if 0 > best_regret:
                    best_regret = 0
                    best_idx = i
                    best_trip_obj = None
                    best_pid = ''
                continue
            delivered = sum(t.amount for t in nw.routes[cid]['trips'])
            is_last = (delivered + amount) >= nw.routes[cid]['total_demand'] - 0.5
            scores = []
            for pid in nw.prob.plants:
                t_list = nw.prob.trucks_by_plant.get(pid, [])
                for tid in t_list:
                    for pc in p_cands:
                        trip = nw.schedule_trip(
                            tid, pid, cid, amount,
                            len(nw.routes[cid]['trips']), rng,
                            pc['pump_ready'], pc['pump_id'], is_last,
                            pc['already_on_site']
                        )
                        if not trip:
                            continue
                        inter_site_penalty = 0
                        if pc.get('from_site') and pc.get('last_cid'):
                            prev_c = nw.prob.customers.get(pc['last_cid'])
                            new_c = nw.prob.customers.get(cid)
                            if prev_c and new_c:
                                d_km = haversine(prev_c['lat'], prev_c['lng'], new_c['lat'], new_c['lng'])
                                inter_site_penalty = max(5, round((d_km / 40) * 60 * 1.2)) * COST_CFG.inter_site_multiplier
                            else:
                                inter_site_penalty = 60 * COST_CFG.inter_site_multiplier
                        elif pc.get('from_site'):
                            inter_site_penalty = 60 * COST_CFG.inter_site_multiplier
                        scores.append({
                            's': trip.batch_start + trip.concrete_age * 0.5 + inter_site_penalty,
                            'trip': trip, 'pid': pid
                        })
            if not scores:
                regret = 0
                chosen_trip = None
                chosen_pid = ''
            elif len(scores) == 1:
                regret = scores[0]['s']
                chosen_trip = scores[0]['trip']
                chosen_pid = scores[0]['pid']
            else:
                scores.sort(key=lambda x: x['s'])
                regret = scores[1]['s'] - scores[0]['s']
                chosen_trip = scores[0]['trip']
                chosen_pid = scores[0]['pid']

            if regret > best_regret:
                best_regret = regret
                best_idx = i
                best_trip_obj = chosen_trip
                best_pid = chosen_pid

        item = pending.pop(best_idx)
        if best_trip_obj:
            nw.routes[item['cid']]['trips'].append(best_trip_obj)
            nw.routes[item['cid']]['pid'] = best_pid
        else:
            nw.removed.append(item)
    return nw


def cheapest_plant_repair(sol: Solution, rng: SeededRNG) -> Solution:
    nw = sol.copy()
    pending = sorted(nw.removed[:], key=lambda x: sol.prob.customers[x['cid']]['tw_start'])
    nw.removed.clear()
    for item in pending:
        cid, amount = item['cid'], item['amount']
        sorted_plants = sorted(nw.prob.plants.keys(), key=lambda pid: nw.prob.dist_km(pid, cid))
        p_cands = pump_candidates(nw, cid)
        if not p_cands:
            nw.removed.append(item)
            continue
        delivered = sum(t.amount for t in nw.routes[cid]['trips'])
        is_last = (delivered + amount) >= nw.routes[cid]['total_demand'] - 0.5
        inserted = False
        for pid in sorted_plants:
            t_list = sorted(nw.prob.trucks_by_plant.get(pid, []),
                            key=lambda tid: nw.truck_free(tid))
            for tid in t_list:
                for pc in p_cands:
                    trip = nw.schedule_trip(
                        tid, pid, cid, amount,
                        len(nw.routes[cid]['trips']), rng,
                        pc['pump_ready'], pc['pump_id'], is_last,
                        pc['already_on_site']
                    )
                    if trip:
                        nw.routes[cid]['trips'].append(trip)
                        nw.routes[cid]['pid'] = pid
                        inserted = True
                        break
                if inserted:
                    break
            if inserted:
                break
        if not inserted:
            nw.removed.append(item)
    return nw


def random_repair(sol: Solution, rng: SeededRNG) -> Solution:
    nw = sol.copy()
    rng.shuffle(nw.removed)
    pending = nw.removed[:]
    nw.removed.clear()
    for item in pending:
        cid, amount = item['cid'], item['amount']
        delivered = sum(t.amount for t in nw.routes[cid]['trips'])
        is_last = (delivered + amount) >= nw.routes[cid]['total_demand'] - 0.5
        res = best_insertion(nw, cid, amount, rng, True, is_last)
        if res:
            nw.routes[cid]['trips'].append(res['trip'])
            nw.routes[cid]['pid'] = res['pid']
        else:
            nw.removed.append({'cid': cid, 'amount': amount})
    return nw


# ═══════════════════════════════════════════════
#  ALNS ANA DÖNGÜSÜ
# ═══════════════════════════════════════════════

def run_alns_engine(cfg: dict, data: dict,
                    on_progress: Callable = None,
                    on_log: Callable = None) -> dict:
    """
    ALNS motorunu çalıştırır.

    cfg: { maxIter, segSize, minRemove, maxRemove, saTemp, saCool,
           makespanWeight, truckActivationCost, fuelCostPerKm,
           waitWeight, pumpIdleWeight, pumpTransferWeight,
           interSiteMultiplier, unservedPenalty }
    data: D objesi (plants, trucks, pumps, customers, solution)
    """
    global _traffic_cache
    _traffic_cache = {}  # her çağrıda temizle

    if on_progress is None:
        on_progress = lambda *a: None
    if on_log is None:
        on_log = lambda msg: None

    # Maliyet parametrelerini güncelle
    COST_CFG.makespan_weight = cfg.get('makespanWeight', 2.5)
    COST_CFG.truck_activation_cost = cfg.get('truckActivationCost', 50)
    COST_CFG.fuel_cost_per_km = cfg.get('fuelCostPerKm', 0.5)
    COST_CFG.wait_weight = cfg.get('waitWeight', 8)
    COST_CFG.pump_idle_weight = cfg.get('pumpIdleWeight', 7)
    COST_CFG.pump_transfer_weight = cfg.get('pumpTransferWeight', 5)
    COST_CFG.inter_site_multiplier = cfg.get('interSiteMultiplier', 5)
    COST_CFG.unserved_penalty = cfg.get('unservedPenalty', 200)

    # Orijinal seyahat sürelerini sakla
    orig_travel_base = {}
    for r in data.get('solution', {}).get('routes', []):
        for t in r.get('trips', []):
            k = f"{t['plant_id']}_{r['customer_id']}"
            base_to = t.get('travel_to_duration', 0) / (t.get('traffic_multiplier_to', 1) or 1)
            if k not in orig_travel_base or base_to < orig_travel_base[k]:
                orig_travel_base[k] = base_to

    prob = Problem(data, orig_travel_base)
    rng = SeededRNG(42)

    # Eski solver4 referansı
    on_log('Eski çözüm (solver4, 120dk limit) referans olarak yükleniyor...')
    solver4 = import_current_solution(prob, rng, data)
    solver4_stats = solver4.stats()
    on_log(f'  Solver4: {solver4_stats["trips"]} sefer, {solver4_stats["m3"]}m³, obj={solver4_stats["obj"]}')

    # Greedy heuristic
    on_log('Greedy heuristic (90dk limit) oluşturuluyor...')
    greedy = build_greedy(prob, rng)
    greedy_stats = greedy.stats()
    on_log(f'  Greedy 90dk: {greedy_stats["trips"]} sefer, {greedy_stats["m3"]}m³, '
           f'%{greedy_stats["pct"]} karşılama, obj={greedy_stats["obj"]}')

    if greedy_stats['pct'] < 100:
        unmet = greedy_stats['demand'] - greedy_stats['m3']
        on_log(f'  ⚠️ {unmet:.0f}m³ talep karşılanamadı')

    init_stats = greedy_stats
    current = greedy.copy()
    best = greedy.copy()
    best_obj = best.objective()
    current_obj = best_obj

    on_log(f'ALNS başlatılıyor... (başlangıç obj={init_stats["obj"]})')

    destroy_ops = [random_removal, worst_cost_removal, related_removal,
                   time_window_removal, truck_chain_removal]
    repair_ops = [greedy_repair, regret2_repair, cheapest_plant_repair, random_repair]

    d_weights = [1.0] * 5
    r_weights = [1.0] * 4
    d_scores = [0.0] * 5
    r_scores = [0.0] * 4
    d_uses = [0] * 5
    r_uses = [0] * 4

    def roulette_select(weights):
        total = sum(weights)
        r_val = rng.next() * total
        cum = 0.0
        for i, w in enumerate(weights):
            cum += w
            if r_val <= cum:
                return i
        return len(weights) - 1

    max_iter = cfg.get('maxIter', 800)
    seg_size = cfg.get('segSize', 100)
    min_remove = cfg.get('minRemove', 2)
    max_remove = cfg.get('maxRemove', 6)
    temp = cfg.get('saTemp', 8.0)
    sa_cool = cfg.get('saCool', 0.9995)

    t0 = time.time()

    for iteration in range(max_iter):
        if (time.time() - t0) > 60:
            break

        n_remove = rng.rand_int(min_remove, max_remove)
        d_idx = roulette_select(d_weights)
        r_idx = roulette_select(r_weights)

        destroyed = destroy_ops[d_idx](current, n_remove, rng)
        candidate = repair_ops[r_idx](destroyed, rng)
        cand_obj = candidate.objective()

        reward = 0
        if cand_obj < best_obj:
            best = candidate.copy()
            best_obj = cand_obj
            current = candidate
            current_obj = cand_obj
            reward = 33
        elif cand_obj < current_obj:
            current = candidate
            current_obj = cand_obj
            reward = 9
        else:
            delta = cand_obj - current_obj
            if delta > 0 and rng.next() < math.exp(-delta / max(temp, 1e-9)):
                current = candidate
                current_obj = cand_obj
                reward = 3

        d_scores[d_idx] += reward
        r_scores[r_idx] += reward
        d_uses[d_idx] += 1
        r_uses[r_idx] += 1
        temp *= sa_cool

        if (iteration + 1) % seg_size == 0:
            for i in range(5):
                if d_uses[i] > 0:
                    d_weights[i] = d_weights[i] * 0.85 + (1 - 0.85) * d_scores[i] / d_uses[i]
            for i in range(4):
                if r_uses[i] > 0:
                    r_weights[i] = r_weights[i] * 0.85 + (1 - 0.85) * r_scores[i] / r_uses[i]
            d_scores = [0.0] * 5
            r_scores = [0.0] * 4
            d_uses = [0] * 5
            r_uses = [0] * 4

        if iteration % 10 == 0:
            on_progress(iteration, max_iter, best_obj, temp)

    elapsed = round(time.time() - t0, 1)
    final_stats = best.stats()
    on_log(f'ALNS tamamlandı ({elapsed}s) – Greedy obj={init_stats["obj"]} → ALNS obj={final_stats["obj"]}')
    on_progress(max_iter, max_iter, best_obj, temp)

    return {
        'solution': best,
        'initial_stats': init_stats,
        'solver4_stats': solver4_stats,
        'final_stats': final_stats,
        'prob': prob,
        'elapsed': elapsed,
    }


# ═══════════════════════════════════════════════
#  ÇÖZÜMÜ D FORMATINA DÖNÜŞTÜRME
# ═══════════════════════════════════════════════

def ts_clock(m: float) -> str:
    return f"{int(m // 60):02d}:{int(m % 60):02d}"


def convert_to_d(sol: Solution, prob: Problem) -> dict:
    new_routes = []

    for cid, r in sol.routes.items():
        if not r['trips']:
            continue
        c = prob.customers[cid]
        plant = prob.plants.get(r['pid']) or prob.plants.get(list(prob.plants.keys())[0])

        d_trips = []
        for t in r['trips']:
            truck = prob.trucks.get(t.tid)
            pl = prob.plants.get(t.pid)
            segments = [
                {'type': 'batching', 'start': round(t.batch_start), 'end': round(t.batch_end), 'label': 'Dolum'},
                {'type': 'travel_to', 'start': round(t.batch_end), 'end': round(t.arrive), 'label': 'Gidiş'},
            ]
            if t.wait > 0.5:
                segments.append({'type': 'waiting', 'start': round(t.arrive),
                                 'end': round(t.unload_start), 'label': 'Bekleme'})
            segments.append({'type': 'unloading', 'start': round(t.unload_start),
                             'end': round(t.unload_end), 'label': 'Boşaltma'})
            segments.append({'type': 'washing', 'start': round(t.unload_end),
                             'end': round(t.wash_end), 'label': 'Yıkama'})
            segments.append({'type': 'travel_back', 'start': round(t.wash_end),
                             'end': round(t.return_plant), 'label': 'Dönüş'})

            d_trips.append({
                'trip_index': t.trip_idx,
                'truck_id': t.tid, 'truck_name': truck.get('name', t.tid) if truck else t.tid,
                'truck_capacity': truck.get('capacity_m3', truck.get('capacity', 9)) if truck else 9,
                'plant_id': t.pid, 'plant_name': pl.get('name', t.pid) if pl else t.pid,
                'plant_lat': pl.get('lat') if pl else None,
                'plant_lng': pl.get('lng') if pl else None,
                'customer_id': cid, 'customer_name': c.get('name', cid),
                'customer_lat': c.get('lat'), 'customer_lng': c.get('lng'),
                'amount': t.amount,
                'batch_start': round(t.batch_start), 'batch_start_clock': ts_clock(t.batch_start),
                'batch_end': round(t.batch_end), 'batch_end_clock': ts_clock(t.batch_end),
                'travel_to_duration': round(t.travel_to),
                'traffic_multiplier_to': round(t.t_mult_to * 100) / 100,
                'arrive_site': round(t.arrive), 'arrive_site_clock': ts_clock(t.arrive),
                'wait_duration': round(t.wait),
                'unload_start': round(t.unload_start), 'unload_start_clock': ts_clock(t.unload_start),
                'unload_end': round(t.unload_end), 'unload_end_clock': ts_clock(t.unload_end),
                'concrete_age': round(t.concrete_age),
                'wash_end': round(t.wash_end),
                'leave_site': round(t.wash_end), 'leave_site_clock': ts_clock(t.wash_end),
                'travel_back_duration': round(t.travel_back_dur),
                'traffic_multiplier_back': round(t.t_mult_back * 100) / 100,
                'return_plant': round(t.return_plant), 'return_plant_clock': ts_clock(t.return_plant),
                'pump_id': t.pump_id or '',
                'segments': segments,
            })

        d_trips.sort(key=lambda x: x['batch_start'])
        new_routes.append({
            'customer_id': cid, 'customer_name': c.get('name', cid),
            'customer_lat': c.get('lat'), 'customer_lng': c.get('lng'),
            'total_demand': c.get('total_demand', 0), 'needs_pump': c.get('needs_pump', False),
            'time_window': c.get('irsaliye', ''), 'plant_id': r['pid'],
            'plant_name': plant.get('name', '') if plant else '',
            'trips': d_trips,
            'served_demand': sum(t['amount'] for t in d_trips),
        })

    # Pompa atamalarını yeniden hesapla
    new_pa = []
    pump_routes = sorted(
        [r for r in new_routes if r['needs_pump'] and r['trips']],
        key=lambda r: r['trips'][0]['unload_start']
    )

    for r in pump_routes:
        c = prob.customers.get(r['customer_id'])
        if not c:
            continue
        av_pumps = [p for p in prob.pumps.values()
                    if (not c.get('required_mobile_pump_type')
                        or p.get('mobile_pump_type') == c['required_mobile_pump_type'])
                    and (not c.get('required_min_pump_capacity')
                         or (p.get('pump_capacity', 0) or 0) >= c.get('required_min_pump_capacity', 0))]
        if not av_pumps:
            continue

        used_pumps: Dict[str, list] = {}
        for pa in new_pa:
            used_pumps.setdefault(pa['pump_id'], []).append(pa)

        best_pump = None
        best_score = float('inf')

        for pump in av_pumps:
            prev_jobs = used_pumps.get(pump['id'], [])
            free_at = SHIFT_START
            last_lat, last_lng = None, None

            if prev_jobs:
                last_job = prev_jobs[-1]
                free_at = last_job['return_plant']
                if r['customer_id'] != last_job['customer_id']:
                    last_cust = prob.customers.get(last_job['customer_id'])
                    if last_cust:
                        last_lat = last_cust['lat']
                        last_lng = last_cust['lng']
                        free_at = last_job.get('teardown_end', free_at) + PUMP_TEARDOWN

            p_plant = prob.plants.get(pump['plant_id'])
            if last_lat is not None:
                c_lat = c.get('lat', 0)
                c_lng = c.get('lng', 0)
                dk2 = haversine(last_lat, last_lng, c_lat, c_lng)
                travel_to_site = max(5, round((dk2 / 40) * 60 * 1.2))
            else:
                travel_to_site = round(prob.travel_time(pump['plant_id'], r['customer_id'], 1.2)) if p_plant else 15

            first_trip = r['trips'][0]
            needed_at = first_trip['unload_start'] - PUMP_SETUP
            depart = max(free_at, needed_at - travel_to_site)
            arr_site = depart + travel_to_site
            setup_end = arr_site + PUMP_SETUP

            if setup_end > first_trip['unload_start'] + 2:
                continue

            is_from_site = last_lat is not None
            inter_site_penalty = (PUMP_SETUP + PUMP_TEARDOWN) * 2 if is_from_site else 0
            score = depart + travel_to_site * 0.5 + len(prev_jobs) * 0.01 + inter_site_penalty
            if score < best_score:
                best_score = score
                best_pump = {'pump': pump, 'travel_to_site': travel_to_site, 'depart': depart}

        if not best_pump:
            continue

        pump = best_pump['pump']
        travel_to_site = best_pump['travel_to_site']
        depart = best_pump['depart']
        p_plant = prob.plants.get(pump['plant_id'])
        first_trip = r['trips'][0]
        last_trip = r['trips'][-1]
        arr_site = depart + travel_to_site
        setup_end = arr_site + PUMP_SETUP
        teardown_start = last_trip['unload_end']
        teardown_end = teardown_start + PUMP_TEARDOWN
        travel_back_site = travel_to_site
        return_plant = teardown_end + travel_back_site

        r_trips = sorted(r['trips'], key=lambda t: t['unload_start'])
        segments = [
            {'type': 'travel_to', 'start': round(depart), 'end': round(arr_site), 'label': 'Gidiş'},
            {'type': 'setup', 'start': round(arr_site), 'end': round(setup_end), 'label': 'Kurulum'},
        ]
        for i, trip in enumerate(r_trips):
            prev_end = setup_end if i == 0 else r_trips[i - 1]['unload_end']
            if trip['unload_start'] > prev_end + 1:
                segments.append({'type': 'waiting', 'start': round(prev_end),
                                 'end': round(trip['unload_start']), 'label': 'Araç Bekleme'})
            segments.append({'type': 'pumping', 'start': round(trip['unload_start']),
                             'end': round(trip['unload_end']), 'label': f'Pompalama {i + 1}'})

        segments.append({'type': 'teardown', 'start': round(teardown_start),
                         'end': round(teardown_end), 'label': 'Söküm'})
        segments.append({'type': 'travel_back', 'start': round(teardown_end),
                         'end': round(return_plant), 'label': 'Dönüş'})

        new_pa.append({
            'pump_id': pump['id'], 'pump_name': pump.get('name', ''),
            'pump_capacity': pump.get('pump_capacity'),
            'pump_plant_id': pump.get('plant_id', ''),
            'pump_plant_name': p_plant.get('name', '') if p_plant else '',
            'pump_plant_lat': p_plant.get('lat') if p_plant else None,
            'pump_plant_lng': p_plant.get('lng') if p_plant else None,
            'customer_id': r['customer_id'], 'customer_name': c.get('name', ''),
            'customer_lat': c.get('lat'), 'customer_lng': c.get('lng'),
            'depart_plant': round(depart), 'depart_plant_clock': ts_clock(depart),
            'travel_to_site': round(travel_to_site),
            'arrive_site': round(arr_site), 'arrive_site_clock': ts_clock(arr_site),
            'setup_end': round(setup_end), 'setup_end_clock': ts_clock(setup_end),
            'teardown_start': round(teardown_start),
            'teardown_end': round(teardown_end), 'teardown_end_clock': ts_clock(teardown_end),
            'return_plant': round(return_plant), 'return_plant_clock': ts_clock(return_plant),
            'total_duration': round(return_plant - depart),
            'segments': segments,
        })

    return {'routes': new_routes, 'pump_assignments': new_pa}
