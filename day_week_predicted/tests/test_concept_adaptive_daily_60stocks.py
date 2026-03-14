#!/usr/bin/env python3
"""
概念板块自适应日预测回测测试 — 60只股票，覆盖15个概念板块

完全自包含脚本，支持两种运行模式：
  python3 day_week_predicted/tests/test_concept_adaptive_daily_60stocks.py --simulate
  python3 day_week_predicted/tests/test_concept_adaptive_daily_60stocks.py  # 需要DB

要求：60只股票 × 15个概念板块(每板块≥4只) × 60天 → 日预测准确率(宽松) ≥ 65%
"""
import json, sys, os, math, random, logging
from collections import defaultdict
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════
# 15个概念板块 × 4只 = 60只股票
# ═══════════════════════════════════════════════════════════════
CB = {
    '人工智能':   {'bc':'BK0800','s':['002230.SZ','300496.SZ','688111.SH','300474.SZ']},
    '新能源汽车': {'bc':'BK0900','s':['002594.SZ','601238.SH','600733.SH','002074.SZ']},
    '半导体':     {'bc':'BK0801','s':['002371.SZ','603986.SH','688012.SH','002049.SZ']},
    '锂电池':     {'bc':'BK0802','s':['300750.SZ','002709.SZ','300014.SZ','002460.SZ']},
    '光伏':       {'bc':'BK0803','s':['601012.SH','300763.SZ','688599.SH','002129.SZ']},
    '医药生物':   {'bc':'BK0804','s':['600276.SH','300760.SZ','603259.SH','600436.SH']},
    '白酒':       {'bc':'BK0805','s':['600519.SH','000858.SZ','000568.SZ','002304.SZ']},
    '军工':       {'bc':'BK0806','s':['600893.SH','600760.SH','002179.SZ','600862.SH']},
    '储能':       {'bc':'BK0807','s':['300274.SZ','002812.SZ','300037.SZ','688390.SH']},
    '机器人':     {'bc':'BK0808','s':['300124.SZ','002747.SZ','688169.SH','300024.SZ']},
    '消费电子':   {'bc':'BK0809','s':['002475.SZ','600745.SH','002241.SZ','300308.SZ']},
    '稀土永磁':   {'bc':'BK0810','s':['600111.SH','300748.SZ','600549.SH','002600.SZ']},
    '化工新材料': {'bc':'BK0811','s':['600309.SH','002440.SZ','600426.SH','002648.SZ']},
    '数据中心':   {'bc':'BK0812','s':['603019.SH','000977.SZ','002236.SZ','002916.SZ']},
    '汽车零部件': {'bc':'BK0813','s':['601799.SH','603596.SH','002920.SZ','600066.SH']},
}
CODES = list(dict.fromkeys(c for v in CB.values() for c in v['s']))

# ═══════════════════════════════════════════════════════════════
# 工具函数
# ═══════════════════════════════════════════════════════════════
def _m(lst): return sum(lst)/len(lst) if lst else 0.0
def _sd(lst):
    if len(lst)<2: return 0.0
    m=_m(lst); return math.sqrt(sum((x-m)**2 for x in lst)/len(lst))
def _sig(x,c=0,s=1):
    try: return 1.0/(1.0+math.exp(-(x-c)/s))
    except: return 0.0 if x<c else 1.0
def _rs(ok,n): return f'{ok}/{n} ({round(ok/n*100,1)}%)' if n>0 else '无数据'
def _ema(d,p):
    if not d: return []
    r=[0.0]*len(d); k=2/(p+1); r[0]=d[0]
    for i in range(1,len(d)): r[i]=d[i]*k+r[i-1]*(1-k)
    return r
def _macd(c,f=12,s=26,sg=9):
    if len(c)<s+sg: return []
    ef=_ema(c,f); es=_ema(c,s); di=[ef[i]-es[i] for i in range(len(c))]; de=_ema(di,sg)
    return [{'D':di[i],'E':de[i],'M':2*(di[i]-de[i])} for i in range(len(c))]
def _kdj(hi,lo,cl,n=9,m1=3,m2=3):
    if len(cl)<n: return []
    r=[]; kp=dp=50.0
    for i in range(len(cl)):
        if i<n-1: r.append({'K':50.0,'D':50.0,'J':50.0}); continue
        hn=max(hi[i-n+1:i+1]); ln=min(lo[i-n+1:i+1])
        rsv=(cl[i]-ln)/(hn-ln)*100 if hn!=ln else 50
        k=(m1-1)/m1*kp+1/m1*rsv; d=(m2-1)/m2*dp+1/m2*k; j=3*k-2*d; kp,dp=k,d
        r.append({'K':round(k,2),'D':round(d,2),'J':round(j,2)})
    return r
def _boll(cl,p=20,mu=2):
    r=[None]*len(cl)
    if len(cl)<p: return r
    for i in range(p-1,len(cl)):
        w=cl[i-p+1:i+1]; mid=sum(w)/p; s=math.sqrt(sum((x-mid)**2 for x in w)/p)
        r[i]={'u':mid+mu*s,'m':mid,'l':mid-mu*s}
    return r

# ═══════════════════════════════════════════════════════════════
# 模拟数据生成
# ═══════════════════════════════════════════════════════════════
def _tdates(s,e):
    d=[]; dt=datetime.strptime(s,'%Y-%m-%d'); ed=datetime.strptime(e,'%Y-%m-%d')
    while dt<=ed:
        if dt.weekday()<5: d.append(dt.strftime('%Y-%m-%d'))
        dt+=timedelta(days=1)
    return d

def _gkl(seed,dates,trend=0.0,vol=2.0):
    rng=random.Random(seed); kl=[]; cl=50+rng.uniform(-20,30)
    for d in dates:
        chg=rng.gauss(0.02+trend*0.3,vol)
        if len(kl)>=3:
            ar=_m([k['cp'] for k in kl[-3:]])
            if ar>1.5: chg-=0.5
            elif ar<-1.5: chg+=0.5
        if len(kl)>=1 and abs(kl[-1]['cp'])>3: chg*=1.2
        try:
            wd=datetime.strptime(d,'%Y-%m-%d').weekday()
            if wd==4: chg-=0.15
            elif wd==0: chg+=0.1
        except: pass
        chg=max(-10,min(10,chg)); op=cl*(1+rng.gauss(0,0.005)); nc=cl*(1+chg/100)
        hi=max(op,nc)*(1+abs(rng.gauss(0,0.005))); lo=min(op,nc)*(1-abs(rng.gauss(0,0.005)))
        v=rng.uniform(50000,500000)*(1+abs(chg)*0.2)
        kl.append({'date':d,'open_price':round(op,2),'close_price':round(nc,2),
                   'high_price':round(hi,2),'low_price':round(lo,2),
                   'trading_volume':round(v),'trading_amount':round(v*nc,2),
                   'change_percent':round(chg,2),'change_hand':round(rng.uniform(0.5,8),2),'cp':round(chg,2)})
        cl=nc
    return kl

def _gbkl(seed,dates,trend=0.0):
    rng=random.Random(seed); kl=[]; cl=1000+rng.uniform(-200,200)
    for d in dates:
        chg=rng.gauss(0.01+trend*0.2,1.5)
        if len(kl)>=3:
            ar=_m([k['change_percent'] for k in kl[-3:]])
            if ar>1.0: chg-=0.3
            elif ar<-1.0: chg+=0.3
        chg=max(-8,min(8,chg)); nc=cl*(1+chg/100)
        kl.append({'date':d,'change_percent':round(chg,2),'close_price':round(nc,2)}); cl=nc
    return kl

def _gmkl(dates):
    rng=random.Random(42); kl=[]; cl=3200.0
    for d in dates:
        chg=rng.gauss(0.01,1.0)
        if len(kl)>=5:
            ar=_m([k['change_percent'] for k in kl[-5:]])
            if ar>0.8: chg-=0.2
            elif ar<-0.8: chg+=0.2
        chg=max(-5,min(5,chg)); nc=cl*(1+chg/100)
        kl.append({'date':d,'change_percent':round(chg,2),'close_price':round(nc,2)}); cl=nc
    return kl

def gen_sim(codes,cb,sd,ed):
    ext=(datetime.strptime(sd,'%Y-%m-%d')-timedelta(days=180)).strftime('%Y-%m-%d')
    td=_tdates(ext,ed); rng=random.Random(123)
    bt={v['bc']:rng.uniform(-0.3,0.3) for v in cb.values()}
    skl={}
    for c in codes:
        t=0.0
        for bn,v in cb.items():
            if c in v['s']: t=bt[v['bc']]; break
        skl[c]=_gkl(hash(c)%2**31,td,t,rng.uniform(1.5,3.0))
    sb={}
    for c in codes:
        b=[]
        for bn,v in cb.items():
            if c in v['s']: b.append({'board_code':v['bc'],'board_name':bn})
        sb[c]=b
    bkm={}
    for bn,v in cb.items():
        bkm[v['bc']]=_gbkl(hash(v['bc'])%2**31,td,bt[v['bc']])
    mkl=_gmkl(td)
    ffm={}
    for c in codes:
        rng2=random.Random(hash(c+'ff')%2**31); fl=[]
        for d in td:
            bn=rng2.gauss(0,5000)
            fl.append({'date':d,'big_net':round(bn,2),'big_net_pct':round(bn/50000*100,2),
                       'main_net_5day':round(rng2.gauss(0,10000),2),'net_flow':round(rng2.gauss(0,8000),2)})
        ffm[c]=list(reversed(fl))
    rng3=random.Random(456); ssm={}
    for c in codes:
        st={}
        for bn,v in cb.items():
            if c in v['s']:
                sc=rng3.uniform(20,80)
                st[v['bc']]={'strength_score':round(sc,1),
                    'strength_level':'强势' if sc>60 else ('弱势' if sc<40 else '中性'),
                    'excess_5d':round(rng3.gauss(0,2),2),'excess_20d':round(rng3.gauss(0,5),2),
                    'excess_total':round(rng3.gauss(0,8),2),'win_rate':round(rng3.uniform(0.3,0.7),3),
                    'rank_in_board':rng3.randint(1,20),'board_total_stocks':rng3.randint(15,50)}
        ssm[c]=st
    return {'stock_klines':skl,'stock_boards':sb,'board_kline_map':bkm,
            'market_klines':mkl,'fund_flow_map':ffm,'stock_strength_map':ssm}
