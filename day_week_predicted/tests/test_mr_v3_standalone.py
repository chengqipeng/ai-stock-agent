#!/usr/bin/env python3
"""
概念板块动量-回归双模型日预测 v3 — 完全独立脚本

用法:
  python3 day_week_predicted/tests/test_mr_v3_standalone.py          # DB模式
  python3 day_week_predicted/tests/test_mr_v3_standalone.py --sim    # 模拟模式

60只股票 × 15概念板块 × 60+天 → 目标65%宽松准确率
"""
import sys, os, json, math, random, logging
from collections import defaultdict
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)

# ── 15板块 × 4只 = 60只 ──
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
SD, ED = '2025-12-10', '2026-03-10'

def _m(a): return sum(a)/len(a) if a else 0.0
def _sd(a):
    if len(a)<2: return 0.0
    m=_m(a); return math.sqrt(sum((x-m)**2 for x in a)/len(a))
def _rs(ok,n): return f'{ok}/{n} ({round(ok/n*100,1)}%)' if n>0 else '-'

# ═══════════════════════════════════════════════════════════
# 模拟数据（精简版，只生成必要天数）
# ═══════════════════════════════════════════════════════════
def _tdays(s, e):
    r=[]; d=datetime.strptime(s,'%Y-%m-%d'); ed=datetime.strptime(e,'%Y-%m-%d')
    while d<=ed:
        if d.weekday()<5: r.append(d.strftime('%Y-%m-%d'))
        d+=timedelta(days=1)
    return r

def _gkl(seed, dates, trend=0.0, vol=2.0):
    """生成带均值回归特性的K线。"""
    rng=random.Random(seed); kl=[]; cl=30+rng.uniform(-10,40)
    for d in dates:
        chg=rng.gauss(0.02+trend*0.2, vol)
        # 均值回归
        if len(kl)>=3:
            a3=sum(k['chg'] for k in kl[-3:])/3
            chg -= a3*0.15
        if len(kl)>=1 and abs(kl[-1]['chg'])>3:
            chg -= kl[-1]['chg']*0.25
        # 连续涨跌回归
        if len(kl)>=3:
            st=0
            for j in range(1,min(6,len(kl)+1)):
                if kl[-j]['chg']>0.2: st+=1
                elif kl[-j]['chg']<-0.2: st-=1
                else: break
            if st>=3: chg-=0.8
            elif st<=-3: chg+=0.8
        chg=max(-10,min(10,chg))
        op=cl*(1+rng.gauss(0,0.003)); nc=cl*(1+chg/100)
        hi=max(op,nc)*(1+abs(rng.gauss(0,0.004)))
        lo=min(op,nc)*(1-abs(rng.gauss(0,0.004)))
        v=rng.uniform(50000,500000)*(1+abs(chg)*0.3)
        kl.append({'date':d,'open':round(op,2),'close':round(nc,2),
                   'high':round(hi,2),'low':round(lo,2),
                   'vol':round(v),'chg':round(chg,2)})
        cl=nc
    return kl

def _gbkl(seed, dates, trend=0.0):
    rng=random.Random(seed); kl=[]; cl=1000+rng.uniform(-200,200)
    for d in dates:
        chg=rng.gauss(0.01+trend*0.15, 1.2)
        if len(kl)>=3: chg-=sum(k['chg'] for k in kl[-3:])/3*0.1
        chg=max(-6,min(6,chg)); nc=cl*(1+chg/100)
        kl.append({'date':d,'chg':round(chg,2),'close':round(nc,2)}); cl=nc
    return kl

def _gmkl(dates):
    rng=random.Random(42); kl=[]; cl=3200.0
    for d in dates:
        chg=rng.gauss(0.01,0.8)
        if len(kl)>=5: chg-=sum(k['chg'] for k in kl[-5:])/5*0.08
        chg=max(-4,min(4,chg)); nc=cl*(1+chg/100)
        kl.append({'date':d,'chg':round(chg,2),'close':round(nc,2)}); cl=nc
    return kl

def gen_sim():
    # 只生成120天lookback + 60天回测 = ~180天
    ext=(datetime.strptime(SD,'%Y-%m-%d')-timedelta(days=120)).strftime('%Y-%m-%d')
    td=_tdays(ext, ED)
    log.info("模拟数据: %d个交易日", len(td))
    rng=random.Random(123)
    bt={v['bc']:rng.uniform(-0.3,0.3) for v in CB.values()}
    skl={}
    for c in CODES:
        t=0.0
        for bn,v in CB.items():
            if c in v['s']: t=bt[v['bc']]; break
        skl[c]=_gkl(hash(c)%2**31, td, t, rng.uniform(1.5,2.8))
    sb={}
    for c in CODES:
        b=[]
        for bn,v in CB.items():
            if c in v['s']: b.append({'board_code':v['bc'],'board_name':bn})
        sb[c]=b
    bkm={}
    for bn,v in CB.items():
        bkm[v['bc']]=_gbkl(hash(v['bc'])%2**31, td, bt[v['bc']])
    mkl=_gmkl(td)
    rng3=random.Random(456); sm={}
    for c in CODES:
        st={}
        for bn,v in CB.items():
            if c in v['s']:
                sc=rng3.uniform(25,75)
                st[v['bc']]={'score':round(sc,1),
                    'level':'强势' if sc>60 else ('弱势' if sc<40 else '中性')}
        sm[c]=st
    return {'stock_klines':skl,'stock_boards':sb,'board_kline_map':bkm,
            'market_klines':mkl,'strength_map':sm}

# ═══════════════════════════════════════════════════════════
# 概念板块确认信号
# ═══════════════════════════════════════════════════════════
def concept_confirm(boards, board_kline_map, market_klines, strength_data, score_date):
    if not boards:
        return {'neutral':True,'up':False,'down':False,'bullish':0.5,'strong':0.5}
    mk={k['date']:k['chg'] for k in market_klines if k['date']<=score_date}
    bup=0; btot=0; exl=[]; ssl=[]
    for b in boards:
        bk=board_kline_map.get(b['board_code'],[])
        br=[k for k in bk if k['date']<=score_date]
        if len(br)<5: continue
        btot+=1
        l5=br[-5:]
        b5=sum(k['chg'] for k in l5)
        m5=sum(mk.get(k['date'],0) for k in l5)
        exl.append(b5-m5)
        if b5>0: bup+=1
        sd=strength_data.get(b['board_code'])
        if sd: ssl.append(sd['score']/100.0)
    if btot==0:
        return {'neutral':True,'up':False,'down':False,'bullish':0.5,'strong':0.5}
    bull=bup/btot; ex=_m(exl); ss=_m(ssl) if ssl else 0.5
    cup=(bull>=0.65 and ex>0.5) or (bull>=0.55 and ss>0.65 and ex>0)
    cdn=(bull<=0.35 and ex<-0.5) or (bull<=0.45 and ss<0.35 and ex<0)
    return {'neutral':not cup and not cdn,'up':cup,'down':cdn,
            'bullish':round(bull,3),'strong':round(ss,3),'excess':round(ex,3)}

# ═══════════════════════════════════════════════════════════
# 模型选择器
# ═══════════════════════════════════════════════════════════
class ModeSel:
    def __init__(self, w=25):
        self.w=w; self.mr=[]; self.rr=[]
    def record(self, mom_ok, rev_ok):
        self.mr.append(mom_ok); self.rr.append(rev_ok)
        if len(self.mr)>self.w: self.mr=self.mr[-self.w:]; self.rr=self.rr[-self.w:]
    @property
    def mom_rate(self):
        return sum(self.mr)/len(self.mr) if len(self.mr)>=5 else 0.5
    @property
    def rev_rate(self):
        return sum(self.rr)/len(self.rr) if len(self.rr)>=5 else 0.5
    @property
    def weights(self):
        mr=self.mom_rate; rr=self.rev_rate
        if abs(mr-rr)<0.08: return 0.5, 0.5
        elif mr>rr:
            w=min(0.8, 0.5+(mr-rr)*2); return w, 1-w
        else:
            w=min(0.8, 0.5+(rr-mr)*2); return 1-w, w

# ═══════════════════════════════════════════════════════════
# 核心预测
# ═══════════════════════════════════════════════════════════
def predict(kl, idx, mkl, cc, ms, sd):
    """单日预测。返回 dict 或 None。"""
    if idx<20: return None
    c=kl[idx]['close']; c1=kl[idx-1]['close']
    if c<=0 or c1<=0: return None
    chg=(c-c1)/c1*100

    # 近期收益率
    rets=[]
    for j in range(min(20,idx)):
        cj=kl[idx-j]['close']; cj1=kl[idx-j-1]['close']
        if cj1>0: rets.append((cj-cj1)/cj1*100)
    if len(rets)<10: return None

    vol=max(0.5, _sd(rets))
    z=chg/vol

    # ── 动量模型 ──
    r3=sum(rets[:3]); r5=sum(rets[:5])
    ms_score=0.0
    if r3>0.5: ms_score+=1.5
    elif r3>0: ms_score+=0.5
    elif r3<-0.5: ms_score-=1.5
    elif r3<0: ms_score-=0.5
    if r5>1.0: ms_score+=1.0
    elif r5<-1.0: ms_score-=1.0
    # 趋势一致性
    up5=sum(1 for r in rets[:5] if r>0.1)
    dn5=sum(1 for r in rets[:5] if r<-0.1)
    if up5>=4: ms_score+=1.0
    elif dn5>=4: ms_score-=1.0
    mom_dir='上涨' if ms_score>0 else '下跌'

    # ── 回归模型 ──
    rv=0.0
    if z>2.0: rv-=3.0
    elif z>1.3: rv-=1.5
    elif z>0.8: rv-=0.5
    elif z<-2.0: rv+=3.0
    elif z<-1.3: rv+=1.5
    elif z<-0.8: rv+=0.5
    # 2日z
    if idx>=2:
        c2=kl[idx-2]['close']
        if c2>0:
            z2=(c-c2)/c2*100/(vol*1.41)
            if z2>1.5: rv-=1.5
            elif z2<-1.5: rv+=1.5
    # 5日z
    if idx>=5:
        c5=kl[idx-5]['close']
        if c5>0:
            z5=(c-c5)/c5*100/(vol*2.24)
            if z5>1.2: rv-=1.0
            elif z5<-1.2: rv+=1.0
    # RSI
    gains=[max(r,0) for r in rets[:14]]
    losses=[max(-r,0) for r in rets[:14]]
    ag=_m(gains); al=max(_m(losses),0.001)
    rsi=100-(100/(1+ag/al))
    if rsi>75: rv-=1.5
    elif rsi>65: rv-=0.5
    elif rsi<25: rv+=1.5
    elif rsi<35: rv+=0.5
    # 连续涨跌
    su=sd2=0
    for j in range(min(10,idx)):
        ij=idx-j
        if ij<=0: break
        if kl[ij]['close']>kl[ij-1]['close']:
            if sd2>0: break
            su+=1
        elif kl[ij]['close']<kl[ij-1]['close']:
            if su>0: break
            sd2+=1
        else: break
    if su>=4: rv-=2.0
    elif su>=3: rv-=1.0
    elif sd2>=4: rv+=2.0
    elif sd2>=3: rv+=1.0
    rev_dir='上涨' if rv>0 else '下跌'

    # ── 成交量 ──
    vt=kl[idx].get('vol',0) or 0
    v5=[kl[idx-j].get('vol',0) or 0 for j in range(min(5,idx+1))]
    av=_m(v5) if v5 else 1
    vr=vt/av if av>0 else 1.0
    vm=vv=0.0
    if chg>0.5 and vr>1.5: vm=0.5
    elif chg>0.5 and vr<0.7: vv=0.5
    elif chg<-0.5 and vr>1.5: vm=0.5
    elif chg<-0.5 and vr<0.7: vv=0.5

    # ── 大盘 ──
    madj=0.0
    mf=[k for k in mkl if k['date']<=sd]
    if len(mf)>=5:
        mt=mf[-1]['chg']
        m5d=sum(k['chg'] for k in mf[-5:])
        if mt<-1.5: madj=0.5
        elif mt>1.5: madj=-0.3
        if m5d>3: madj-=0.3
        elif m5d<-3: madj+=0.3

    # ── 融合 ──
    mw, rw = ms.weights
    am=ms_score+vm; ar=rv+vv+madj
    combined=am*mw+ar*rw

    # 概念确认
    cadj=0.0
    if not cc.get('neutral',True):
        if cc['up']:
            cadj=0.8 if combined>0 else 0.3
        elif cc['down']:
            cadj=-0.8 if combined<0 else -0.3
    final=combined+cadj

    # 阈值
    th=max(0.1, min(0.4, vol/5.0))
    if final>th: d='上涨'
    elif final<-th: d='下跌'
    else:
        if abs(rv)>1.0: d=rev_dir
        elif abs(ms_score)>1.0: d=mom_dir
        elif cc.get('up'): d='上涨'
        elif cc.get('down'): d='下跌'
        else: d='上涨'

    # 极端修正
    if su>=4 and d=='上涨' and abs(final)<2.0: d='下跌'
    elif sd2>=4 and d=='下跌' and abs(final)<2.0: d='上涨'
    if z>2.5 and d=='上涨': d='下跌'
    elif z<-2.5 and d=='下跌': d='上涨'

    conf='high' if abs(final)>2.0 else ('mid' if abs(final)>0.8 else 'low')
    return {'dir':d,'final':round(final,3),'mom':mom_dir,'rev':rev_dir,
            'mode':'mom' if mw>rw+0.05 else ('rev' if rw>mw+0.05 else 'blend'),
            'conf':conf,'z':round(z,2),'rsi':round(rsi,1),'su':su,'sd':sd2}

# ═══════════════════════════════════════════════════════════
# DB数据加载
# ═══════════════════════════════════════════════════════════
def load_db_data():
    from decimal import Decimal
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
    from dao import get_connection
    def _f(v):
        if v is None: return 0.0
        if isinstance(v, Decimal): return float(v)
        return float(v)

    ext=(datetime.strptime(SD,'%Y-%m-%d')-timedelta(days=150)).strftime('%Y-%m-%d')
    c6=[c.split('.')[0] for c in CODES]
    fm={c.split('.')[0]:c for c in CODES}
    conn=get_connection(use_dict_cursor=True); cur=conn.cursor()
    try:
        # K线
        aq=list(set(c6+CODES)); ph=','.join(['%s']*len(aq))
        cur.execute(f"SELECT stock_code,`date`,open_price,close_price,high_price,"
                    f"low_price,trading_volume,change_percent FROM stock_kline "
                    f"WHERE stock_code IN ({ph}) AND `date`>=%s AND `date`<=%s "
                    f"ORDER BY stock_code,`date`", (*aq,ext,ED))
        skl={}
        for r in cur.fetchall():
            f=fm.get(r['stock_code'],r['stock_code'])
            if f not in skl: skl[f]=[]
            skl[f].append({'date':r['date'],'open':_f(r['open_price']),'close':_f(r['close_price']),
                           'high':_f(r['high_price']),'low':_f(r['low_price']),
                           'vol':_f(r['trading_volume']),'chg':_f(r['change_percent'])})
        # 板块映射
        sb=defaultdict(list); abc=set()
        ph2=','.join(['%s']*len(c6))
        cur.execute(f"SELECT stock_code,board_code,board_name FROM stock_concept_board_stock "
                    f"WHERE stock_code IN ({ph2})", tuple(c6))
        for r in cur.fetchall():
            f=fm.get(r['stock_code'],r['stock_code'])
            sb[f].append({'board_code':r['board_code'],'board_name':r['board_name']})
            abc.add(r['board_code'])
        # 板块K线
        bkm=defaultdict(list)
        if abc:
            bl=list(abc); ph3=','.join(['%s']*len(bl))
            cur.execute(f"SELECT board_code,`date`,change_percent,close_price FROM concept_board_kline "
                        f"WHERE board_code IN ({ph3}) AND `date`>=%s AND `date`<=%s "
                        f"ORDER BY board_code,`date`", (*bl,ext,ED))
            for r in cur.fetchall():
                bkm[r['board_code']].append({'date':r['date'],'chg':_f(r['change_percent']),
                                             'close':_f(r['close_price'])})
        # 大盘
        cur.execute("SELECT `date`,change_percent,close_price FROM stock_kline "
                    "WHERE stock_code='000001.SH' AND `date`>=%s AND `date`<=%s ORDER BY `date`",
                    (ext,ED))
        mkl=[{'date':r['date'],'chg':_f(r['change_percent']),'close':_f(r['close_price'])}
             for r in cur.fetchall()]
        # 强弱
        sm=defaultdict(dict)
        cur.execute(f"SELECT stock_code,board_code,strength_score,strength_level "
                    f"FROM stock_concept_strength WHERE stock_code IN ({ph2})", tuple(c6))
        for r in cur.fetchall():
            f=fm.get(r['stock_code'],r['stock_code'])
            sm[f][r['board_code']]={'score':_f(r['strength_score']),'level':r['strength_level']}
    finally:
        cur.close(); conn.close()
    log.info("[DB] %d只K线, %d只有板块, %d板块K线, 大盘%d天",
             len(skl), sum(1 for c in CODES if c in sb),
             sum(1 for bc in abc if bc in bkm), len(mkl))
    return {'stock_klines':dict(skl),'stock_boards':dict(sb),
            'board_kline_map':dict(bkm),'market_klines':mkl,'strength_map':dict(sm)}

# ═══════════════════════════════════════════════════════════
# 回测主循环
# ═══════════════════════════════════════════════════════════
def run_backtest(data):
    t0=datetime.now()
    skl=data['stock_klines']; sb=data['stock_boards']
    bkm=data['board_kline_map']; mkl=data['market_klines']
    sm=data['strength_map']

    all_r=[]; summaries=[]; bstats=defaultdict(lambda:{'n':0,'lok':0,'sok':0,'s':set()})
    skip=0

    for code in CODES:
        kl=skl.get(code,[])
        kl=[k for k in kl if (k.get('vol') or 0)>0]
        if len(kl)<80: skip+=1; continue
        si=None
        for i,k in enumerate(kl):
            if k['date']>=SD: si=i; break
        if si is None or si<40: skip+=1; continue

        boards=sb.get(code,[]); sd_data=sm.get(code,{})
        sel=ModeSel(25); days=[]

        for i in range(si, len(kl)-1):
            sdate=kl[i]['date']
            if sdate>ED: break
            cc=concept_confirm(boards, bkm, mkl, sd_data, sdate)
            p=predict(kl, i, mkl, cc, sel, sdate)
            if p is None: continue

            base=kl[i]['close']; nxt=kl[i+1]
            if base<=0: continue
            achg=round((nxt['close']-base)/base*100, 2)
            aup=achg>=0; pup=p['dir']=='上涨'
            lok=(pup and aup) or (not pup and not aup)
            if achg>0.3: adir='上涨'
            elif achg<-0.3: adir='下跌'
            else: adir='横盘'
            sok=p['dir']==adir

            # 更新模型选择器
            mok=(p['mom']=='上涨' and aup) or (p['mom']=='下跌' and not aup)
            rok=(p['rev']=='上涨' and aup) or (p['rev']=='下跌' and not aup)
            sel.record(mok, rok)

            days.append({'code':code,'date':sdate,'pred':p['dir'],'achg':achg,
                         'adir':adir,'lok':lok,'sok':sok,'conf':p['conf'],
                         'mode':p['mode'],'boards':[b['board_name'] for b in boards[:3]],
                         'cc_neutral':cc.get('neutral',True)})
            for b in boards[:5]:
                bn=b['board_name']
                bstats[bn]['n']+=1
                if lok: bstats[bn]['lok']+=1
                if sok: bstats[bn]['sok']+=1
                bstats[bn]['s'].add(code)

        all_r.extend(days)
        if days:
            nd=len(days); lo=sum(1 for d in days if d['lok']); so=sum(1 for d in days if d['sok'])
            summaries.append({'code':code,'boards':', '.join([b['board_name'] for b in boards[:3]]),
                              'days':nd,'loose':_rs(lo,nd),'strict':_rs(so,nd),
                              'mom_r':f'{round(sel.mom_rate*100,1)}%',
                              'rev_r':f'{round(sel.rev_rate*100,1)}%',
                              'mode':sel.weights})
            log.info("%s [%s] %d天 宽松%.1f%% 动量%.1f%% 回归%.1f%%",
                     code, ', '.join([b['board_name'] for b in boards[:2]]),
                     nd, lo/nd*100, sel.mom_rate*100, sel.rev_rate*100)

    elapsed=(datetime.now()-t0).total_seconds()
    if not all_r:
        return {'status':'no data','elapsed':round(elapsed,1),'skipped':skip}

    # 汇总
    tot=len(all_r); tlok=sum(1 for r in all_r if r['lok']); tsok=sum(1 for r in all_r if r['sok'])

    # 按模式
    mst=defaultdict(lambda:{'n':0,'lok':0})
    for r in all_r:
        m=r['mode']; mst[m]['n']+=1
        if r['lok']: mst[m]['lok']+=1

    # 按置信度
    cst=defaultdict(lambda:{'n':0,'lok':0})
    for r in all_r:
        c=r['conf']; cst[c]['n']+=1
        if r['lok']: cst[c]['lok']+=1

    # 概念确认效果
    wc=[r for r in all_r if not r['cc_neutral']]
    nc=[r for r in all_r if r['cc_neutral']]

    # 板块Top20
    sbs=sorted(bstats.items(), key=lambda x:x[1]['n'], reverse=True)[:20]

    # 排序
    summaries.sort(key=lambda x:float(x['loose'].split('(')[1].replace('%)','')), reverse=True)
    a65=sum(1 for s in summaries if float(s['loose'].split('(')[1].replace('%)', ''))>=65)
    a60=sum(1 for s in summaries if float(s['loose'].split('(')[1].replace('%)', ''))>=60)

    return {
        '回测类型':'概念板块动量-回归双模型 v3',
        '耗时(秒)':round(elapsed,1),
        '回测区间':f'{SD} ~ {ED}',
        '股票数':len(CODES),'有效股票数':len(CODES)-skip,'跳过股票数':skip,
        '总样本数':tot,
        '总体准确率(宽松)':_rs(tlok,tot),
        '总体准确率(严格)':_rs(tsok,tot),
        '达标':{'≥65%':a65,'≥60%':a60,'总有效':len(summaries)},
        '按模式':{m:{'样本':s['n'],'宽松':_rs(s['lok'],s['n'])} for m,s in mst.items()},
        '按置信度':{c:{'样本':s['n'],'宽松':_rs(s['lok'],s['n'])} for c,s in cst.items()},
        '概念确认效果':{
            '有确认':{'样本':len(wc),'宽松':_rs(sum(1 for r in wc if r['lok']),len(wc))},
            '无确认':{'样本':len(nc),'宽松':_rs(sum(1 for r in nc if r['lok']),len(nc))},
        },
        '按板块(Top20)':{bn:{'股票':len(s['s']),'样本':s['n'],'宽松':_rs(s['lok'],s['n'])}
                         for bn,s in sbs},
        '各股票汇总':[{'股票代码':s['code'],'概念板块':s['boards'],'天数':s['days'],
                       '准确率(宽松)':s['loose'],'准确率(严格)':s['strict'],
                       '动量胜率':s['mom_r'],'回归胜率':s['rev_r']} for s in summaries],
    }

# ═══════════════════════════════════════════════════════════
# main
# ═══════════════════════════════════════════════════════════
def main():
    sim='--sim' in sys.argv
    print(f"{'='*70}")
    print(f"概念板块动量-回归双模型日预测 v3")
    print(f"股票: {len(CODES)}, 板块: {len(CB)}, 区间: {SD}~{ED}")
    print(f"模式: {'模拟' if sim else 'DB'}, 目标: ≥65%")
    print(f"{'='*70}")

    if sim:
        data=gen_sim()
    else:
        data=load_db_data()

    result=run_backtest(data)

    out='data_results/backtest_mr_v3_60stocks_result.json'
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out,'w',encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print(f"\n{'='*70}")
    print(f"总样本: {result.get('总样本数',0)}")
    print(f"有效股票: {result.get('有效股票数',0)}, 跳过: {result.get('跳过股票数',0)}")
    print(f"总体准确率(宽松): {result.get('总体准确率(宽松)','?')}")
    print(f"总体准确率(严格): {result.get('总体准确率(严格)','?')}")
    print(f"耗时: {result.get('耗时(秒)',0)}秒")

    for k in ['按模式','按置信度','概念确认效果']:
        v=result.get(k,{})
        if v:
            print(f"\n{k}:")
            for kk,vv in v.items(): print(f"  {kk}: {vv}")

    sl=result.get('各股票汇总',[])
    if sl:
        print(f"\nTop10:")
        for s in sl[:10]:
            print(f"  {s['股票代码']} [{s['概念板块'][:20]}] 宽松{s['准确率(宽松)']} 动量{s['动量胜率']} 回归{s['回归胜率']}")
        if len(sl)>5:
            print(f"\nBottom5:")
            for s in sl[-5:]:
                print(f"  {s['股票代码']} [{s['概念板块'][:20]}] 宽松{s['准确率(宽松)']}")

    pct_str=result.get('总体准确率(宽松)','0/0 (0%)')
    pct=float(pct_str.split('(')[1].replace('%)','')) if '(' in pct_str else 0
    print(f"\n{'='*70}")
    if pct>=65: print(f"✅ 达标! {pct}% ≥ 65%")
    else: print(f"⚠️ 未达标: {pct}% < 65%")
    print(f"结果: {out}")

if __name__=='__main__':
    main()
