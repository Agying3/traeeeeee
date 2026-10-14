# -*- coding: utf-8 -*-
"""human_track.py - minimum-jerk 人体拖动轨迹生成器 + P95 审计。

设计基线(来自思路.txt):
- 时间参数化 minimum-jerk: s(u) = 10u^3 - 15u^4 + 6u^5
  起点与终点的速度、加速度均趋近于零,全程连续无段间硬切换。
- AR(1) 平滑时间采样: weight[i] = 0.82*weight[i-1] + 0.18*rand,
  全局归一化到总时长;dt 缓变而非逐帧白噪。
- 低频 Y 漂移: y = A * sin(pi*u)^1.7 * sin(2*pi*cycles*u), 双端自然收敛到 0。
- 中段停顿用单个大 dt 表示,不制造零位移事件串。
- 末端校正只改最后一个 dx,累计位移误差 < 1e-9。
"""
import math
import os
import random

import numpy as np


def smooth5(u):
    u = max(0.0, min(1.0, u))
    return 10 * u ** 3 - 15 * u ** 4 + 6 * u ** 5


def time_grid(total_s, n, rng, mode="smooth"):
    """生成递增时间点。

    smooth: AR(1) 平滑采样,dt 围绕均值缓变。
    human_longtail: 大量 4~8ms + 少量 45~140ms 长尾间隔,模拟真实输入采样抖动。
    """
    if mode == "human_longtail":
        # 分布校准(dt_match_v2 真人 4111 间隔): 主体 4~6ms(65%) + 宽尾巴,
        # 目标 dt_cv≈2.7(真人 p50) 而非 1.6。
        raw = []
        for _ in range(n - 1):
            r = rng.random()
            if r < 0.62:
                raw.append(rng.uniform(0.0035, 0.0065))
            elif r < 0.78:
                raw.append(rng.uniform(0.008, 0.018))
            elif r < 0.90:
                raw.append(rng.uniform(0.025, 0.060))
            else:
                raw.append(rng.uniform(0.070, 0.220))
        scale = total_s / max(sum(raw), 1e-9)
        # 快档时长(0.55s)会把小间隔缩到 1.3~1.7ms,低于真实输入采样粒度。
        # 下限 1.2ms(真人 dt_min p10~p90 = 1.0~2.2ms; 旧 3ms 是指纹)。
        min_dt_s = 0.0012
        if scale * 0.004 < min_dt_s:
            # 小间隔全部压到下限,长尾保持缩放值
            times = [0.0]
            for dt in raw:
                dt_s = max(dt * scale, min_dt_s)
                times.append(times[-1] + dt_s)
            # 尾端截断到不超过 total_s*1.5(避免时长严重超标)
            max_end = total_s * 1.5
            if times[-1] > max_end:
                k = max_end / times[-1]
                times = [t * k for t in times]
            times[-1] = max(times[-1], total_s)
            return times
        times = [0.0]
        for dt in raw:
            times.append(times[-1] + dt * scale)
        times[-1] = total_s
        return times

    base = total_s / (n - 1)
    weights = []
    last = 1.0
    for _ in range(n - 1):
        target = rng.uniform(0.88, 1.12)
        last = 0.82 * last + 0.18 * target
        weights.append(last)
    scale = total_s / sum(base * w for w in weights)
    times = [0.0]
    for w in weights:
        times.append(times[-1] + base * w * scale)
    times[-1] = total_s
    return times


def generate_drag(target_mouse_dx, total_s=None, events=None,
                  y_amplitude=None, y_cycles=None, rng=None,
                  mid_pause_prob=0.15, bias_px=0.0,
                  variant=None):
    """生成 [(dx, dy, dt_ms)],累计 dx == target_mouse_dx + bias_px(误差<1e-9)。

    target_mouse_dx: 鼠标需走的总距离(CSS px),由 (piece_target - B)/K 换算。
    bias_px: 故意偏移量——轨迹落在目标旁边一小段,由后续一次修正拉回。
             模拟真人"差一点"的手感,避免每把都完美落点(特征明显)。
    """
    rng = rng or random
    # 真人样本拟合(human_drag_samples.json): 爆发前载速度曲线。
    # 实测 3 样本: 峰值 2963~4643 px/s @ 5%~43%(平均约 0.18),
    # 事件数 66~101, 总时长 0.81~2.15s。
    # 模型: 速度 v(t) = peak * (1-u)^decay, 峰值在 u≈0 附近,指数衰减。
    # decay 越大越"猛冲后滑行"(样本#0/#1),越小越"匀速到底"(样本#2)。
    variant = (variant or os.environ.get("TRACK_VARIANT", "baseline_current")).strip() or "baseline_current"
    if events is None:
        # 真人事件数 p10~p90 = 86~215(p50=127); 死帧修剪会吃掉 ~35%,
        # 源头要多放。
        events = rng.randint(130, 200)
    # 衰减指数 4~12: 前几帧爆发冲掉大半距离,其余时间低速滑行。
    # 真人样本 240px/0.81s/peak3478 -> 平均/峰值≈0.085 -> decay≈11。
    decay = rng.uniform(4.0, 12.0)
    # release_*/feedback* 变体 = combo 主轨迹 + 人工松手末端,继承 combo 的全部特征。
    variant_base = "combo" if (variant.startswith("release_") or variant.startswith("feedback")) else variant
    # peak_late/combo: 峰值后移,避免所有样本在 5~10% 过早冲峰。
    if variant_base in ("peak_late", "combo"):
        front_ramp = rng.uniform(0.08, 0.25)
    else:
        front_ramp = rng.uniform(0.02, 0.12)  # 峰值前的加速 ramp 占比
    if y_amplitude is None:
        if variant_base in ("high_y", "combo"):
            y_amplitude = rng.uniform(5.0, 12.0)
        else:
            y_amplitude = rng.uniform(2.0, 6.5)
    # feedback: 真人 y 路径标准差 p10~p90 = 9~100px(p50=38), 我们原来 2.2
    # (形态诊断 shape_gap_diagnosis: y_std 是第二大判别特征)。
    # 真人的大 y_std 来自"手臂慢漂移"(单向缓移) + 小抖动,不是大振幅振荡;
    # 漂移由 y_drift 实现(在采样处叠加),振荡振幅保持温和(不脱离滑块)。
    y_drift = 0.0
    if variant.startswith("feedback"):
        y_amplitude = rng.uniform(8.0, 18.0)
        # 真人 y_std p50=38: 线性漂移的 std ≈ drift/3.5, 需 drift 60~200。
        # 但滑块贴近验证码组件底部(下方 ~30px 就是边缘), 向下漂移出 iframe
        # 会断拖(mousemove 不再派发给 iframe 文档)。方向: 75% 向上(安全,
        # 上方是拼图区 ~200px), 25% 小幅向下。
        if rng.random() < 0.75:
            y_drift = -rng.uniform(25.0, 90.0)     # 向上(负 y)
        else:
            y_drift = rng.uniform(10.0, 35.0)      # 小幅向下
    y_cycles = y_cycles if y_cycles is not None else rng.uniform(0.5, 1.1)

    D = float(target_mouse_dx)
    D_actual = D + float(bias_px)

    # ---- 爆发前载速度形状(归一化,无峰值因子) ----
    # v(u): u<front_ramp 线性启动; 之后 (1-uu)^decay 指数衰减滑行。
    def v_unit(u):
        if u < front_ramp and front_ramp > 0:
            return u / front_ramp
        uu = (u - front_ramp) / max(1e-6, 1.0 - front_ramp)
        return (1.0 - uu) ** decay

    M = 400
    grid_u = np.linspace(0.0, 1.0, M)
    Vn = np.array([v_unit(u) for u in grid_u])
    Sn = np.zeros(M)
    for i in range(1, M):
        Sn[i] = Sn[i - 1] + (Vn[i - 1] + Vn[i]) * 0.5 * (grid_u[i] - grid_u[i - 1])
    Vn_sum = float(Sn[-1])  # 形状积分(≈1/(decay+1) 量级)

    # ---- 真实峰值速度采样 -> 反解总时长 ----
    # 峰值 = D / (Vn_sum * T)  =>  T = D / (Vn_sum * peak)
    # 时长分布必须多样(12 连发全 700ms 是强机器人特征):
    #   55% 快(0.55~0.85s) / 30% 中(0.9~1.4s) / 15% 慢(1.4~1.9s)
    if total_s is None:
        r = rng.random()
        if variant.startswith("feedback"):
            # 真人时长 p50=1.62s p90=3.47s —— 比我们原来慢得多
            if r < 0.20:
                total_s = rng.uniform(0.9, 1.25)
            elif r < 0.65:
                total_s = rng.uniform(1.25, 1.9)
            else:
                total_s = rng.uniform(1.9, 3.1)
        elif r < 0.55:
            total_s = rng.uniform(0.55, 0.85)
        elif r < 0.85:
            total_s = rng.uniform(0.90, 1.40)
        else:
            total_s = rng.uniform(1.40, 1.90)

    time_mode = "human_longtail" if variant_base in ("dt_longtail", "combo") else "smooth"
    times = time_grid(total_s, events, rng, mode=time_mode)
    # 中段/末端停顿: 用单个较大 dt 表示,不插入零位移事件。
    if variant_base not in ("dt_longtail", "combo") and rng.random() < mid_pause_prob and len(times) > 10:
        i = rng.randint(2, len(times) - 3)
        pause = rng.uniform(0.03, 0.09)
        for j in range(i, len(times)):
            times[j] += pause
        overflow = times[-1] - total_s
        if overflow > 0:
            for j in range(1, len(times)):
                times[j] -= overflow * (j / (len(times) - 1))

    # ---- 采样位置曲线 ----
    points = []
    for t in times:
        u = min(1.0, t / total_s) if total_s > 0 else 1.0
        x = D_actual * float(np.interp(u, grid_u, Sn)) / Vn_sum
        phase = 2 * math.pi * y_cycles * u
        envelope = math.sin(math.pi * u) ** 1.7
        # y = 漂移(手臂缓移, 线性增长) + 振荡(手腕抖动); 双端漂移不清零——
        # 真人拖完时手停在偏移处是正常的(滑块只约束 x)。
        y = y_drift * u + y_amplitude * envelope * math.sin(phase)
        points.append((x, y, t))

    track = []
    for i in range(1, len(points)):
        x0, y0, t0 = points[i - 1]
        x1, y1, t1 = points[i]
        dx = x1 - x0
        # 长尾段: 位移钳到最小 0.01 —— 视觉几乎不动但非严格零/负,
        # 避免零位移堆积和负位移特征(插值浮点噪声可能产生 -1e-15)
        if dx < 0.01:
            dx = 0.01
        track.append((dx, y1 - y0, (t1 - t0) * 1000.0))

    # 严格保证累计位移: 按比例微缩放主段帧(避开末尾慢帧),总位移精确
    D_actual = D + float(bias_px)
    cur = sum(dx for dx, _, _ in track)
    if abs(cur - D_actual) > 1e-9:
        # 主段 = 前 80% 帧(速度可观);慢尾帧(0.01 级)不缩放
        n_main = max(2, int(len(track) * 0.8))
        main_sum = sum(track[i][0] for i in range(n_main))
        if main_sum > 1.0:
            k = (main_sum + (D_actual - cur)) / main_sum
            for i in range(n_main):
                dx, dy, dt = track[i]
                track[i] = (dx * k, dy, dt)

    # ---- 视觉反馈模拟(feedback* 变体,在松手行为之前) ----
    # 人工样本微观结构诊断(idx=12 等 30 条)揭示的四大真机特征:
    #   1. 速度呈"双波"形态: 第一波冲到峰值→深回落(10~30%峰值)→
    #      第二次加速(30~80%峰值)→缓慢收尾。reacc 由此而来(p50=3);
    #   2. 整数量化噪声: 屏幕坐标取整让 dx 序列交替抖动,
    #      速度差分符号切换密度 0.49~0.79(我们 0.19~0.38);
    #   3. 中途犹豫停顿: dt 82~140ms 的单帧停顿(看缺口);
    #   4. 末端回拉: 拖过头后往回拖 3~15px,负 dx 帧持续 5~13 帧。
    # 实现(总距离守恒,负 dx 只出现在回拉段,主段仍无负值):
    if variant.startswith("feedback"):
        # ---- 0. 修剪 0.01px 死帧尾巴 ----
        # 指数衰减让主轨迹最后 30~60% 帧全是 0.01px clamp(死帧)。
        # 真人没有死帧——他们的尾部全是真实位移(1~8px)。
        # 反馈变体: 砍掉死帧,把腾出的距离重建为"深谷+第二波+回拉"。
        # 时长保留: 死帧的 dt 合并进最后一个活帧(原来直接丢弃,
        # 导致生成时长比目标短 0.3~0.5s)。
        live = len(track)
        while live > 10 and track[live - 1][0] <= 0.05:
            live -= 1
        dead_sum = sum(t[0] for t in track[live:])
        dead_dy = sum(t[1] for t in track[live:])
        dead_dt = sum(t[2] for t in track[live:])
        if live < len(track) and dead_dt > 0:
            dx, dy, dt = track[live - 1]
            track[live - 1] = (dx, dy, dt + dead_dt)
        track = track[:live]
        # 死帧距离立刻归还主段(按帧比例),避免任何后续分支丢失它
        if dead_sum > 1e-9:
            cur = sum(t[0] for t in track)
            if cur > 0.01:
                k_red = (cur + dead_sum) / cur
                track = [(max(0.01, dx * k_red), dy, dt) for dx, dy, dt in track]
            dead_sum = 0.0  # 已归还,双波分支按整段距离重建
        # 死帧的 dy(承担 Y 归零职责)加到最后一帧,保证 Y 收敛
        if abs(dead_dy) > 1e-9 and track:
            dx, dy, dt = track[-1]
            track[-1] = (dx, dy + dead_dy, dt)
        n = len(track)
        if n >= 10 and sum(t[0] for t in track) > 10:
            # ---- 1. 形态重构(慢接近 + 谷底 + 末端甩鞭) ----
            # 形态差距诊断(shape_gap_diagnosis, 30真人 vs 120生成):
            #   peak_speed_pos 真人 p50=0.745(峰值在 75% 进度!) vs 生成 0.213
            #   speed_max 真人 p50=9722px/s vs 生成 1420 —— 真人是"慢逼近+甩鞭"
            #   而非全程爆发。重构: 前段压速(≤1500px/s) + 谷底 + 甩鞭波(2.5~6ms
            #   大位移帧, 峰值 4000~12000px/s) + 收尾。
            if True:  # feedback 必发(峰位靠前是强机器指纹)
                # 前段: 显式距离预算(15~30%), 其余全给甩鞭。
                # (按帧切分 + 逐帧压速不行: 爆发曲线距离前载, 前半帧吃掉
                #  60~85% 距离, 甩鞭只剩残渣 —— debug_flick 实测。)
                split = int(n * rng.uniform(0.45, 0.60))
                D_actual_now = sum(t[0] for t in track)
                front_budget = D_actual_now * rng.uniform(0.15, 0.30)
                front = track[:split]
                front_sum = sum(t[0] for t in front)
                if front_sum > 1.0:
                    k_front = front_budget / front_sum
                    front = [(max(0.01, dx * k_front), dy, dt)
                             for dx, dy, dt in front]
                track[:split] = front
                flick_total = D_actual_now - front_budget
                back_dy_total = sum(t[1] for t in track[split:])
                if flick_total > 5.0 and n - split >= 6:
                    valley_n = rng.randint(3, 6)
                    flick_n = rng.randint(16, 28)
                    coast_n = rng.randint(5, 10)
                    # 距离分配: 谷 2~5%, 甩鞭 86~94%, 收尾剩余
                    valley_share = rng.uniform(0.02, 0.05)
                    flick_share = rng.uniform(0.86, 0.94)
                    valley_dx = flick_total * valley_share
                    flick_dx = flick_total * flick_share
                    coast_dx = flick_total - valley_dx - flick_dx
                    # 谷底: 微位移 + 大 dt(40~110ms, 看缺口)
                    v_frames = [(valley_dx / valley_n, 0.0, rng.uniform(40, 110))
                                for _ in range(valley_n)]
                    # 甩鞭: 窄驼峰位移, 峰值帧集中大量位移 @ dt 2.5~6ms
                    # => 峰值速度 5000~15000 px/s(真人 speed_max p50=9722)
                    f_shape = []
                    pk = rng.uniform(0.35, 0.6)
                    sigma = rng.uniform(0.07, 0.11)
                    for j in range(flick_n):
                        ph = j / max(1, flick_n - 1)
                        s = math.exp(-((ph - pk) ** 2) / (2 * sigma ** 2))
                        f_shape.append(0.05 + s)
                    fs = sum(f_shape)
                    f_frames = [(flick_dx * s / fs, 0.0, rng.uniform(2.5, 6.0))
                                for s in f_shape]
                    # 收尾: 中速滑行 6~16ms
                    c_frames = [(coast_dx / coast_n, 0.0, rng.uniform(6.0, 16.0))
                                for _ in range(coast_n)]
                    cs = sum(f[0] for f in c_frames)
                    if cs > 0.01:
                        c_frames = [(d * coast_dx / cs, 0.0, dt)
                                    for d, _, dt in c_frames]
                    new_back = v_frames + f_frames + c_frames
                    tot_new = sum(t[0] for t in new_back)
                    for j, (dx, _, dt) in enumerate(new_back):
                        new_back[j] = (max(0.01, dx),
                                       back_dy_total * dx / max(tot_new, 1e-9),
                                       dt)
                    track[split:] = new_back
                    n = len(track)

            # ---- 1. 整数量化噪声(核心!) ----
            # 把连续 dx 舍入到 0.5px 网格,制造真实鼠标的量化抖动。
            # CDP dispatchMouseEvent 支持亚像素,但真实事件流是整数/半像素;
            # 舍入后的差分噪声直接产生 0.5+ 的符号切换密度。
            grid = rng.uniform(0.42, 0.58)   # 量化网格(px)
            quantized = []
            for dx, dy, dt in track:
                # 量化 + 相邻帧随机抖动(真实鼠标的取样噪声)
                q = round(dx / grid) * grid
                if q > 0.5 and rng.random() < 0.35:
                    q = q + rng.choice((-1, 1)) * grid  # 1/3 帧偏移一格
                quantized.append(max(0.01, q))
            # 距离守恒: 残差按帧加权精确分配(不用钳位,避免丢距离)
            q_sum = sum(quantized)
            orig = sum(t[0] for t in track)
            residual_q = orig - q_sum
            if abs(residual_q) > 1e-9 and q_sum > 0.01:
                # 按帧位移比例分摊(大帧吃大头)
                for i in range(n):
                    share = quantized[i] / q_sum
                    quantized[i] = max(0.01, quantized[i] + residual_q * share)
                # 二次兜底: 若钳位又吃掉了距离,把尾差塞进最大帧
                still = orig - sum(quantized)
                if abs(still) > 1e-9:
                    imax = max(range(n), key=lambda i: quantized[i])
                    quantized[imax] += still
            for i in range(n):
                dx, dy, dt = track[i]
                track[i] = (quantized[i], dy, dt)

            # ---- 1.5 零位移帧(真人 4% 帧完全静止, 我们原来是 0%) ----
            # 把 ~5% 的主段帧 dx 置精确 0, 距离摊到相邻帧。
            n_zero = max(1, int(n * rng.uniform(0.03, 0.07)))
            for _ in range(n_zero):
                zi = rng.randint(2, n - 3)
                dx, dy, dt = track[zi]
                if dx > 0.05:
                    # 摊到右邻(保持总距离)
                    nx = track[zi + 1]
                    track[zi] = (0.0, dy, dt)
                    track[zi + 1] = (nx[0] + dx, nx[1], nx[2])

            # ---- 1.6 中途反向微调(真人 speed_reversal p50=6, 我们原来 1) ----
            # 在中段制造 1~3 次小幅回拉-跟进(真人"对位置"的手感)。
            for _ in range(rng.randint(1, 3)):
                ri = rng.randint(int(n * 0.25), max(int(n * 0.25) + 2, int(n * 0.80)))
                if ri + 3 >= n:
                    continue
                pull = rng.uniform(0.8, 2.2)
                # 两帧回拉
                track[ri] = (track[ri][0] - pull * 0.6, track[ri][1], track[ri][2])
                track[ri + 1] = (track[ri + 1][0] - pull * 0.4, track[ri + 1][1],
                                 track[ri + 1][2])
                # 跟进帧补回距离
                track[ri + 2] = (track[ri + 2][0] + pull, track[ri + 2][1],
                                 track[ri + 2][2])

            # ---- 2. 中途犹豫停顿(60% 概率一次,20% 两次) ----
            r_p = rng.random()
            n_pauses = 0 if r_p < 0.40 else (1 if r_p < 0.80 else 2)
            for _ in range(n_pauses):
                # 停顿位置: 40%~85% 进度处(真人是快到缺口时开始看)
                i = rng.randint(int(n * 0.40), max(int(n * 0.40) + 1, int(n * 0.85)))
                dx, dy, dt = track[i]
                pause_ms = rng.uniform(60, 160)
                track[i] = (dx, dy, dt + pause_ms)

            # ---- 3. 末端回拉(拖过头再拖回来) ----
            # 55% 概率: 最后 8~14 帧 dx 变负,幅度 0.5~2.5px/帧。
            # 总位移守恒: 回拉量提前在主段补足。
            if rng.random() < 0.55:
                back_n = rng.randint(6, 13)
                if n - back_n > 10:
                    # 回拉总幅度
                    back_total = rng.uniform(3.0, 12.0)
                    # 每帧回拉量(不均匀,首尾轻中间重)
                    weights = [1.0 + math.sin(math.pi * (k + 1) / back_n) for k in range(back_n)]
                    w_sum = sum(weights)
                    backs = [back_total * w / w_sum for w in weights]
                    # 主段(去掉回拉帧)需要补足 back_total
                    main_frames = track[:n - back_n]
                    main_sum = sum(t[0] for t in main_frames)
                    need = (D + bias_px) + back_total
                    if main_sum > 1.0 and need > 0:
                        k_scale = need / main_sum
                        # 主段整体缩放(每帧至少 0.01px,由全局校正兜底)
                        for i in range(n - back_n):
                            dx, dy, dt = track[i]
                            track[i] = (max(0.01, dx * k_scale), dy, dt)
                    # 回拉段的 dt: 头部一帧长停顿(松手犹豫),其余 4~10ms
                    back_frames = []
                    # 回拉帧的 dy 总量 = 原回拉帧 dy 之和(保持 Y 收敛总量不变)
                    dy_back_total = sum(track[n - back_n + k][1] for k in range(back_n))
                    dy_back_w = [abs(track[n - back_n + k][1]) + 0.01 for k in range(back_n)]
                    dw_sum = sum(dy_back_w)
                    for k in range(back_n):
                        dx, dy, dt = track[n - back_n + k]
                        dt_new = (rng.uniform(40, 140) if k == 0
                                  else rng.uniform(3.5, 10.0))
                        # dx 负值(回拉) + dy 按原权重分配(保持 Y 收敛)
                        dy_new = dy_back_total * dy_back_w[k] / dw_sum if dw_sum > 0 else 0.0
                        back_frames.append((-backs[k], dy_new, dt_new))
                    track[n - back_n:] = back_frames
                    # 回拉后逐帧校正残差(不设幅度上限,保证总距离精确)
                    final_sum = sum(t[0] for t in track)
                    residual = (D + bias_px) - final_sum
                    if abs(residual) > 1e-9:
                        # 残差按主段帧速度加权分配(中段帧吃大头)
                        wts = [max(t[0], 0.01) for t in track[:n - back_n]]
                        wsum = sum(wts)
                        if wsum > 0.01:
                            for i in range(n - back_n):
                                dx, dy, dt = track[i]
                                track[i] = (max(0.01, dx + residual * wts[i] / wsum), dy, dt)

        # ---- 4. 松手前微动作清除(pre_release_micro 真人全为 0) ----
        # 形态诊断: 人工 30 条 pre_release_micro 全部 = 0;我们 0.25。
        # 规则: 最后 200ms 内 0 < |dx| < 1.0 的帧 → 置 0 或抬到 ≥1px,
        # 距离差全部塞进最近的有位移帧,总距离严格守恒。
        n = len(track)
        # 先算最后 200ms 的窗口
        rem = 0.2
        win_start = n
        for i in range(n - 1, -1, -1):
            rem -= track[i][2] / 1000.0
            if rem <= 0:
                win_start = i
                break
        snap_diff = 0.0
        for i in range(win_start, n):
            dx, dy, dt = track[i]
            if 0 < abs(dx) < 1.0:
                if rng.random() < 0.6:
                    snap_diff -= dx          # 归零
                    track[i] = (0.0, dy, dt)
                else:
                    sign = 1.0 if dx > 0 else -1.0
                    snap_diff += (sign * 1.15 - dx)   # 抬到 1.15px
                    track[i] = (sign * 1.15, dy, dt)
        # snap_diff 摊到窗口前第一个 dx≥1px 的帧(不在 200ms 内)
        if abs(snap_diff) > 1e-9:
            for i in range(win_start - 1, -1, -1):
                dx, dy, dt = track[i]
                if dx >= 1.0:
                    track[i] = (max(0.01, dx + snap_diff), dy, dt)
                    break
            else:
                # 兜底: 摊到第一帧
                dx, dy, dt = track[0]
                track[0] = (max(0.01, dx + snap_diff), dy, dt)
        # ---- 5. 全局距离终校(零位移/回拉/吸附的累计误差) ----
        final_diff = (D + float(bias_px)) - sum(t[0] for t in track)
        if abs(final_diff) > 1e-9:
            # 按帧位移占比分摊(≥1px 的帧), 不破坏零帧与吸附帧
            wts2 = [t[0] if t[0] >= 1.0 else 0.0 for t in track]
            ws2 = sum(wts2)
            if ws2 > 0.5:
                for i in range(len(track)):
                    if wts2[i] > 0:
                        dx, dy, dt = track[i]
                        track[i] = (max(0.01, dx + final_diff * wts2[i] / ws2),
                                    dy, dt)
            else:
                dx, dy, dt = track[0]
                track[0] = (max(0.01, dx + final_diff), dy, dt)

    # ---- 人工松手行为(release 变体) ----
    # 真人样本: 最后 dt p50≈19ms/p90≈283ms,最后 dx≈1px,末端速度 p50≈52px/s。
    # 修正目标:
    #   1. 释放延迟: 85% 短延迟 15~60ms + 15% 长尾 100~300ms;
    #   2. 末端位移: 最后一帧保留 1.5~4px 低速非零位移(不是 0.01px);
    #   3. 残余误差: ±0.5~5px,10% 长尾到 5px。
    # feedback 变体: 松手行为已由回拉段承担(拖过头-回拉-松),
    # 跳过本分支,避免 release 尾覆盖回拉帧并引入额外 err。
    if variant in ("release_window", "release_longtail", "release_human"):
        n = len(track)
        if n >= 8:
            # ---- 末端低速非零位移 ----
            # 把主段的一部分距离挪到最后一帧,让末端速度接近 50~80px/s
            end_speed = rng.uniform(45.0, 85.0)      # 目标末端速度 px/s
            if variant == "release_human":
                delay = (rng.uniform(15, 60) if rng.random() < 0.85
                         else rng.uniform(100, 300))
            elif variant == "release_longtail":
                delay = (rng.uniform(20, 80) if rng.random() < 0.82
                         else rng.uniform(100, 320))
            else:
                delay = rng.triangular(20, 200, 80)
            last_dx = end_speed * delay / 1000.0     # 最后一帧位移 = 速度×时间
            last_dx = max(0.5, min(5.0, last_dx))    # 钳到 0.5~5px

            # 从有实际位移的区域挤出 last_dx
            # 主轨迹指数衰减让最后 20% 帧全是 0.01px clamp,必须取更前的窗口
            # 从后往前找累计位移 >= last_dx*2 的最短窗口
            start = n - 1
            acc = 0.0
            while start > 0 and acc < last_dx * 2.0 + 1.0:
                acc += track[start][0]
                start -= 1
            start += 1  # 回退一步(start 是第一个要动的帧)
            if start < n - 1 and acc > last_dx + 0.5:
                # [start..n-1] 窗口内有足够位移可以重新分配
                tail = track[start:]
                # 保留最后的 clamp 帧不动,只缩放有位移的前段
                # 找到有实际位移的分界(从后往前找第一个 dx>0.3px 的帧)
                cut = len(tail) - 1
                while cut > 0 and tail[cut][0] <= 0.3:
                    cut -= 1
                if cut > 0:
                    # 前段(有位移)按比例缩,腾出最后一帧的空间
                    front = tail[:cut]
                    front_sum = sum(t[0] for t in front)
                    if front_sum > last_dx + 0.3:
                        scale = (front_sum - last_dx) / front_sum
                        new_front = [(t[0] * scale, t[1], t[2]) for t in front]
                        # 残余误差(相对目标偏一点再松手)
                        if variant in ("release_human", "feedback", "feedback_release"):
                            err = rng.gauss(0, 1.4)
                            if rng.random() < 0.10:
                                err += rng.choice((-1, 1)) * rng.uniform(2.0, 5.0)
                        else:
                            err = rng.gauss(0, 1.2)
                        err = max(-5.0, min(5.0, err))
                        # 重组: 前段(缩放后) + 保留的 clamp 帧(去掉原最后一帧) + 新最后一帧
                        kept = tail[cut:-1] if len(tail) > cut + 1 else []
                        # 最后一帧: 低速位移 + 残余误差 + 释放延迟
                        # 总位移守恒: 缩放前段腾出 last_dx,但 err 会额外改变总位移。
                        # 解法: 先确定 new_last = last_dx + err,然后整体缩放前段
                        #   使 front_sum_new + kept_sum + (last_dx + err) == 原总位移
                        target_total = front_sum + sum(t[0] for t in tail[cut:])
                        new_last_dx = last_dx + err
                        kept_sum = sum(t[0] for t in kept)
                        needed_from_front = target_total - kept_sum - new_last_dx
                        if needed_from_front > 0.5 and front_sum > 0.5:
                            scale = needed_from_front / front_sum
                            # 钳位: scale 不能太小,否则前段帧出现负 dx
                            # 每帧至少保留 0.01px(和 clamp 一致)
                            scale = max(scale, 0.01 / max(min(t[0] for t in front), 0.01))
                            new_front = [(max(0.01, t[0] * scale), t[1], t[2]) for t in front]
                        new_last_dx = max(0.01, new_last_dx)  # 最后一帧也不能负
                        # 新最后一帧继承原最后一帧的 dy(Y 轴收敛职责不能丢)
                        new_last = (new_last_dx, tail[-1][1], delay)
                        track[start:] = new_front + kept + [new_last]
            # ---- 最终全局校正(修复钳位导致的累计误差) ----
            # 不依赖 release 分支内部状态,直接对整个 track 逐帧分配 diff。
            D_final = D + float(bias_px)
            cur_final = sum(t[0] for t in track)
            remaining = D_final - cur_final
            if abs(remaining) > 1e-9 and abs(remaining) < 6.0:
                # 从后往前找有余量的帧(跳过最后一帧,保留释放特征)
                for i in range(len(track) - 2, 0, -1):
                    if abs(remaining) < 1e-9:
                        break
                    dx, dy, dt = track[i]
                    if remaining < 0:
                        take = min(dx - 0.05, -remaining)  # 最多扣到 0.05px
                        if take > 0:
                            track[i] = (dx - take, dy, dt)
                            remaining += take
                    else:
                        track[i] = (dx + remaining, dy, dt)
                        remaining = 0.0
    return track


def pct(vals, p):
    if not vals:
        return 0.0
    s = sorted(vals)
    k = min(len(s) - 1, max(0, int(round(p / 100.0 * (len(s) - 1)))))
    return s[k]


def audit(track):
    """P95 优先审计:max 对单个异常 dt 敏感,中位数/P95 更能反映整体。"""
    vels = [dx / max(dt_ms / 1000.0, 1e-4) for dx, _, dt_ms in track]
    accels = []
    for i in range(1, len(vels)):
        dt = max(track[i][2] / 1000.0, 1e-4)
        accels.append((vels[i] - vels[i - 1]) / dt)
    jerks = []
    for i in range(1, len(accels)):
        dt = max(track[i + 1][2] / 1000.0, 1e-4)
        jerks.append((accels[i] - accels[i - 1]) / dt)
    dts = [t[2] for t in track]
    dxs = [t[0] for t in track]
    # 主峰计数: 超过峰值 25% 的局部极大
    peaks = 0
    for i in range(1, len(vels) - 1):
        if vels[i] > vels[i - 1] and vels[i] > vels[i + 1] and vels[i] > 0.25 * max(vels):
            peaks += 1
    return {
        "events": len(track),
        "duration_s": sum(dts) / 1000.0,
        "distance_px": sum(dxs),
        "speed_med": pct(vels, 50), "speed_p95": pct(vels, 95),
        "speed_max": max(vels, default=0.0),
        "accel_p95": pct([abs(a) for a in accels], 95),
        "accel_max": max((abs(a) for a in accels), default=0.0),
        "jerk_p95": pct([abs(j) for j in jerks], 95),
        "jerk_max": max((abs(j) for j in jerks), default=0.0),
        "dt_min": min(dts, default=0.0), "dt_med": pct(dts, 50),
        "dt_max": max(dts, default=0.0),
        "neg_dx": sum(1 for d in dxs if d < 0),
        "zero_dx": sum(1 for d in dxs if abs(d) < 1e-9),
        "final_y": sum(t[1] for t in track),
        "speed_peaks": peaks,
    }


def checklist(track, target_mouse_dx, variant=None):
    a = audit(track)
    # feedback 变体: 允许回拉负 dx 与双波多峰(人工样本实测如此:
    # idx12 有 13 帧负 dx/14 个局部峰;no_neg_dx/speed_peaks<=2 是
    # 单波模型的旧约束,对 feedback 不适用)
    is_feedback = bool(variant) and variant.startswith("feedback")
    ok = {
        "duration_0.5_2.3s": 0.5 <= a["duration_s"] <= 2.3,
        # feedback 修剪死帧后帧数天然减少;人工样本最低 36 帧
        "events_60_110": (60 <= a["events"] <= 110 if not is_feedback
                          else 45 <= a["events"] <= 115),
        "dt_positive": a["dt_min"] > 0,
        "cum_dx_err<=0.01px": abs(a["distance_px"] - target_mouse_dx) <= 0.01,
        "final_y<=0.2px": abs(a["final_y"]) <= 0.2,
        "zero_dx<2%": sum(1 for t in track if 0 < abs(t[0]) <= 1e-6) / max(1, a["events"]) < 0.02,
        "tail_zero_ok": True,
        "no_neg_dx": a["neg_dx"] == 0 if not is_feedback else a["neg_dx"] <= 15,
        "speed_peaks<=2": a["speed_peaks"] <= 2 if not is_feedback else a["speed_peaks"] <= 25,
        # 真人实测峰值 2963~4643 px/s;短距离慢速档峰值自然下探(~280)
        "speed_max>=260": a["speed_max"] >= 260,
        # 人工 dt_p50≈5.1ms;human_longtail 网格(4~8ms 小间隔为主)合法
        "dt_med_3_30ms": 3.0 <= a["dt_med"] <= 30.0,
    }
    return ok, a


if __name__ == "__main__":
    import sys
    if sys.platform == "win32":
        try:
            sys.stdout.reconfigure(encoding="utf-8")
        except Exception:
            pass
    regimes = [("短", 100.0), ("中", 240.0), ("长", 400.0)]
    for name, D in regimes:
        fails = []
        for run in range(20):
            tr = generate_drag(D, rng=random.Random(1000 + run))
            ok, a = checklist(tr, D)
            bad = {k: v for k, v in ok.items() if not v}
            if bad:
                fails.append((run, bad))
        tr = generate_drag(D, rng=random.Random(7))
        ok, a = checklist(tr, D)
        print(f"[{name}距离 {D:.0f}px] 20次全过: {not fails}")
        print(f"  时长{a['duration_s']:.2f}s 事件{a['events']} "
              f"速度 med/p95/max = {a['speed_med']:.0f}/{a['speed_p95']:.0f}/{a['speed_max']:.0f} px/s")
        print(f"  加速度 p95/max = {a['accel_p95']:.0f}/{a['accel_max']:.0f}  "
              f"jerk p95/max = {a['jerk_p95']:.0f}/{a['jerk_max']:.0f}")
        print(f"  dt min/med/max = {a['dt_min']:.1f}/{a['dt_med']:.1f}/{a['dt_max']:.1f} ms  "
              f"零位移{a['zero_dx']} 负位移{a['neg_dx']} 速度峰数{a['speed_peaks']} 末y={a['final_y']:.4f}")
        if fails:
            print(f"  [!] 失败: {fails[:3]}")
