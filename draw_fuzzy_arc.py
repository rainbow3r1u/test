import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle
import matplotlib.font_manager as fm
import numpy as np

# 加载中文字体
font_path = '/home/ubuntu/crypto-scanner/SimHei.ttf'
prop_title = fm.FontProperties(fname=font_path, size=18, weight='bold')
prop_h1 = fm.FontProperties(fname=font_path, size=14, weight='bold')
prop_text = fm.FontProperties(fname=font_path, size=11)
prop_small = fm.FontProperties(fname=font_path, size=9)

# 设置画布
fig, ax = plt.subplots(figsize=(15, 9), facecolor='#0d1117')
ax.set_facecolor('#0d1117')

# ================= 模拟真实波动的价格数据 (带噪音的圆弧底) =================
# 左侧下跌 (不再是完美平滑，而是震荡下跌)
x_left = np.arange(0, 15)
y_left = 100 - 15 * np.sin(x_left / 14 * np.pi / 2) + np.random.normal(0, 1.5, 15)

# 底部盘整 (不再是一条直线，允许在箱体内波动)
x_bottom = np.arange(15, 20)
y_bottom = np.random.uniform(83.5, 86.5, 5) 

# 右侧反弹 (允许中间夹杂小阴线回调)
x_right = np.arange(20, 25)
y_right = np.array([86, 88.5, 88.0, 90.5, 93]) # 注意 88.5 到 88.0 是一个小幅回调

x = np.concatenate([x_left, x_bottom, x_right])
y = np.concatenate([y_left, y_bottom, y_right])

# 绘制 K线
for i in range(len(x)):
    curr_c = y[i]
    if i == 0:
        prev_c = curr_c + 2 
    else:
        prev_c = y[i-1]
        
    is_bullish = curr_c > prev_c
    color = '#7ee787' if is_bullish else '#f85149'
    
    bottom = min(prev_c, curr_c)
    height = max(abs(curr_c - prev_c), 0.3)
    
    # 随机添加上下影线
    high = max(prev_c, curr_c) + np.random.uniform(0, 1.5)
    low = min(prev_c, curr_c) - np.random.uniform(0, 1.5)
    
    ax.plot([i, i], [low, high], color=color, linewidth=2)
    ax.add_patch(Rectangle((i - 0.3, bottom), 0.6, height, color=color))

# ================= 标注参数化容错区间 =================

# 1. 左侧下跌参数化
ax.plot(x_left, y_left + 2, color='#58a6ff', linestyle='--', linewidth=2, alpha=0.5)
ax.text(6, 102, "一、左侧下跌 (容错参数化)\n[min_drop_pct] 累计跌幅阈值 (如: > 6%)\n[min_drop_bars] 至少经过几根K线 (如: > 8根)\n不再要求每一根都平滑，只看起点和终点", 
        color='#58a6ff', fontproperties=prop_h1, ha='center', bbox=dict(facecolor='#161b22', edgecolor='#58a6ff', alpha=0.7))

# 2. 底部箱体参数化
ax.add_patch(Rectangle((14.2, 82), 5.6, 5.5, fill=True, facecolor='#d29922', alpha=0.1, edgecolor='#d29922', linestyle='--', linewidth=2))
ax.text(17, 78, "二、底部盘整箱体 (容错参数化)\n[bottom_box_pct] 箱体最大振幅 (如: < 2%)\n[bottom_min_bars] 盘整最少K线数 (如: >= 3根)\n不再要求单根实体极小，只要最高/最低价锁在盒子里即可", 
        color='#d29922', fontproperties=prop_text, ha='center', va='top', bbox=dict(facecolor='#161b22', edgecolor='#d29922', alpha=0.7))

# 3. 右侧反弹容错参数化
ax.add_patch(Rectangle((19.5, 85.5), 5, 8.5, fill=False, edgecolor='#7ee787', linestyle='-', linewidth=2))
ax.annotate('允许夹杂\n小阴线回调', xy=(22, 88), xytext=(24, 85),
            color='#f85149', fontproperties=prop_text,
            arrowprops=dict(facecolor='#f85149', edgecolor='#f85149', arrowstyle='->', alpha=0.7))

ax.text(22, 95, "三、右侧反弹 (容错参数化)\n[rebound_bars] 检查窗口 (如: 最近5根)\n[min_bull_ratio] 阳线占比 (如: 4根阳1根阴)\n[max_drawdown] 允许的最大单根回调 (如: < 1%)\n[min_total_rebound] 累计反弹幅度 (如: > 3%)", 
        color='#7ee787', fontproperties=prop_text, ha='center', va='bottom', bbox=dict(facecolor='#161b22', edgecolor='#7ee787', alpha=0.7))

# 设置坐标轴
ax.set_xlim(-2, 33)
ax.set_ylim(70, 110)
ax.axis('off')

plt.suptitle("实盘量化策略图解：圆弧底 (参数化容错版)", color='white', y=0.96, fontproperties=prop_title)
plt.tight_layout()
plt.savefig('/home/ubuntu/crypto-scanner/static/fuzzy_arc.png', dpi=200, bbox_inches='tight', facecolor='#0d1117')
