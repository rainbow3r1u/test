import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle
import matplotlib.font_manager as fm
import os

# 加载下载的字体
font_path = '/home/ubuntu/crypto-scanner/SimHei.ttf'
prop = fm.FontProperties(fname=font_path)
prop_title = fm.FontProperties(fname=font_path, size=18)
prop_label = fm.FontProperties(fname=font_path, size=14)
prop_desc = fm.FontProperties(fname=font_path, size=12)
prop_small = fm.FontProperties(fname=font_path, size=10)

os.makedirs('/home/ubuntu/crypto-scanner/static', exist_ok=True)

fig, ax = plt.subplots(figsize=(10, 6), facecolor='#0d1117')
ax.set_facecolor('#0d1117')

# [开盘, 最高, 最低, 收盘, 标题, 状态, 说明, 颜色]
scenarios = [
    [100, 105, 99, 104, "1. 正常稳步抬升", "通过", "实体 > 40%\n单根涨幅 < 8%", '#7ee787'],
    [104, 115, 103, 105, "2. 长上影线/骗炮", "淘汰", "实体比例 = 8%\n(< 40%)", '#f85149'],
    [104, 122, 103, 120, "3. 加速赶顶/爆拉", "淘汰", "单根涨幅 = 15%\n(> 8%)", '#f85149']
]

for i, data in enumerate(scenarios):
    o, h, l, c, title, status, desc, col = data
    
    # 影线
    ax.plot([i, i], [l, h], color=col, linewidth=3)
    
    # 实体
    bottom = min(o, c)
    height = max(abs(c - o), 0.5) 
    ax.add_patch(Rectangle((i - 0.15, bottom), 0.3, height, color=col))
    
    # 文字注释
    ax.text(i, h + 2, title, color='white', ha='center', va='bottom', fontproperties=prop_label)
    ax.text(i, l - 2, status, color=col, ha='center', va='top', fontproperties=prop_label)
    ax.text(i, l - 5, desc, color='#8b949e', ha='center', va='top', fontproperties=prop_desc)
    
    # 价格标签
    ax.text(i + 0.2, o, f'开盘: {o}', color='gray', va='center', fontproperties=prop_small)
    ax.text(i + 0.2, c, f'收盘: {c}', color='white', va='center', fontproperties=prop_small)

ax.set_xlim(-0.5, 2.5)
ax.set_ylim(85, 135)
ax.axis('off')
plt.suptitle("稳步抬升 PRO - 新增过滤机制示意图", color='#58a6ff', y=0.98, fontproperties=prop_title)
plt.tight_layout()
plt.savefig('/home/ubuntu/crypto-scanner/static/pro_demo.png', dpi=200, bbox_inches='tight', facecolor='#0d1117')
print("Image saved to /home/ubuntu/crypto-scanner/static/pro_demo.png")
