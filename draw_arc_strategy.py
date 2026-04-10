import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle, Arc, Polygon
import matplotlib.font_manager as fm
import numpy as np

# 加载中文字体
font_path = '/home/ubuntu/crypto-scanner/SimHei.ttf'
prop_title = fm.FontProperties(fname=font_path, size=18, weight='bold')
prop_h1 = fm.FontProperties(fname=font_path, size=14, weight='bold')
prop_text = fm.FontProperties(fname=font_path, size=11)
prop_small = fm.FontProperties(fname=font_path, size=9)

# 设置画布
fig, ax = plt.subplots(figsize=(14, 8), facecolor='#0d1117')
ax.set_facecolor('#0d1117')

# ================= 模拟价格数据 (圆弧底形态) =================
# 左侧下跌 (陡峭 -> 趋缓) -> 底部盘整 -> 右侧反弹
x_left = np.arange(0, 15)
y_left = 100 - 15 * np.sin(x_left / 14 * np.pi / 2)  # 从100跌到85左右

x_bottom = np.arange(15, 20)
y_bottom = np.random.uniform(84.8, 85.2, 5) # 底部盘整

x_right = np.arange(20, 24)
y_right = np.array([86, 87.5, 89.2, 91]) # 4连阳反弹

x = np.concatenate([x_left, x_bottom, x_right])
y = np.concatenate([y_left, y_bottom, y_right])

# 绘制 K线 (不考虑影线，仅开盘收盘)
for i in range(len(x)):
    curr_c = y[i]
    if i == 0:
        prev_c = curr_c + 2 # 第一根默认阴线
    else:
        prev_c = y[i-1]
        
    # 底部盘整区特殊处理极小实体
    if 15 <= i <= 18:
        prev_c = curr_c + np.random.uniform(-0.1, 0.1)
        
    is_bullish = curr_c > prev_c
    color = '#7ee787' if is_bullish else '#f85149'
    
    bottom = min(prev_c, curr_c)
    height = max(abs(curr_c - prev_c), 0.2)
    
    # 陡峭段实体大，趋缓段实体小
    if i < 7: height *= 1.5
    if 10 <= i < 15: height *= 0.5
    
    ax.add_patch(Rectangle((i - 0.3, bottom), 0.6, height, color=color))

# ================= 标注阶段与逻辑 =================

# 1. 左侧圆弧形下跌
ax.plot(x_left, y_left + 1, color='#58a6ff', linestyle='--', linewidth=2, alpha=0.7)
ax.text(5, 101, "一、左侧圆弧形下跌\n(核回归平滑曲线)\n累计跌幅 ≥ 8%", color='#58a6ff', fontproperties=prop_h1, ha='center')

# 陡峭段与趋缓段标注
ax.annotate('陡峭段 (斜率1.5%~3%)\n大实体', xy=(3, 96), xytext=(-1, 92),
            color='#f85149', fontproperties=prop_text,
            arrowprops=dict(facecolor='#f85149', edgecolor='#f85149', arrowstyle='->', alpha=0.7))
            
ax.annotate('趋缓段 (斜率0.3%~1%)\n实体由大转小', xy=(12, 88), xytext=(6, 85),
            color='#ffa657', fontproperties=prop_text,
            arrowprops=dict(facecolor='#ffa657', edgecolor='#ffa657', arrowstyle='->', alpha=0.7))

# H1 和 Ln 标记
H1_y = 100
Ln_y = 85
ax.axhline(H1_y, color='gray', linestyle=':', alpha=0.5)
ax.text(-1, H1_y, "H1 (弧形最高点)", color='white', fontproperties=prop_text, va='center')

ax.axhline(Ln_y, color='gray', linestyle=':', alpha=0.5)
ax.text(-1, Ln_y, "Ln (弧底最低点)", color='white', fontproperties=prop_text, va='center')

# 半弦长
ax.annotate('', xy=(0, 93), xytext=(15, 93), arrowprops=dict(arrowstyle='<->', color='white', alpha=0.5))
ax.text(7.5, 93.5, "半弦长 d > 10根K线", color='white', fontproperties=prop_small, ha='center')

# 2. 底部止跌确认
ax.add_patch(Rectangle((14.5, 84), 4.5, 2, fill=False, edgecolor='#d29922', linestyle='--', linewidth=2))
ax.text(17, 82, "二、底部止跌确认\n1. 极小实体盘整 (≤0.3%)\n2. 成交量极度萎缩 (<0.5*MA20)\n3. 两根阳线反抽", 
        color='#d29922', fontproperties=prop_text, ha='center', va='top')

# 3. 右侧反弹确认
ax.add_patch(Rectangle((19.5, 85.5), 4, 6, fill=False, edgecolor='#7ee787', linestyle='-', linewidth=2))
ax.text(21.5, 93, "三、右侧反弹确认\n1. 连续4根阳线\n2. 收盘价逐根抬高\n3. 单根涨幅 0.3%~2%\n4. 累计反弹 ≥ 2%", 
        color='#7ee787', fontproperties=prop_text, ha='center', va='bottom')

# 4. 触发信号区
ax.plot(24, 91, marker='*', color='yellow', markersize=15)
ax.text(24.5, 91, "信号触发！\n(满足所有形态与环境条件)", color='yellow', fontproperties=prop_h1, va='center')

# 5. 过滤条件说明 (右上角)
info_text = (
    "【全局环境过滤】\n"
    "• 价格 > 200日均线 (大趋势向上)\n"
    "• VIX < 30 (市场不处于极度恐慌)\n"
    "• 仅看开盘/收盘价，无视影线干扰"
)
ax.text(26, 102, info_text, color='#a5d6ff', fontproperties=prop_text, 
        bbox=dict(facecolor='#161b22', edgecolor='#30363d', boxstyle='round,pad=0.5'))

# 设置坐标轴
ax.set_xlim(-2, 32)
ax.set_ylim(75, 105)
ax.axis('off')

plt.suptitle("量化策略图解：圆弧底(Cup & Handle)右侧突破形态", color='white', y=0.95, fontproperties=prop_title)
plt.tight_layout()
plt.savefig('/home/ubuntu/crypto-scanner/static/arc_strategy.png', dpi=200, bbox_inches='tight', facecolor='#0d1117')
