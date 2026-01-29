#!/usr/bin/env python3
"""Generate app icon for XGB 轉換工具."""

from PIL import Image, ImageDraw, ImageFont
import subprocess
import tempfile
from pathlib import Path

SIZE = 1024
OUT_DIR = Path(__file__).parent


def rounded_rect(draw, xy, radius, fill):
    x0, y0, x1, y1 = xy
    draw.rounded_rectangle(xy, radius=radius, fill=fill)


def make_icon():
    img = Image.new("RGBA", (SIZE, SIZE), (0, 0, 0, 0))
    draw = ImageDraw.Draw(img)

    # 背景 — 圓角矩形，漸層感用疊色模擬
    bg_color = (59, 130, 246)  # 藍色
    rounded_rect(draw, (0, 0, SIZE, SIZE), radius=200, fill=bg_color)

    # 上半部疊一層亮色
    overlay = Image.new("RGBA", (SIZE, SIZE), (0, 0, 0, 0))
    od = ImageDraw.Draw(overlay)
    od.rounded_rectangle(
        (0, 0, SIZE, SIZE // 2 + 100),
        radius=200,
        fill=(255, 255, 255, 30),
    )
    img = Image.alpha_composite(img, overlay)
    draw = ImageDraw.Draw(img)

    # 文件圖示 — 白色圓角矩形（放大版）
    doc_x, doc_y = 150, 70
    doc_w, doc_h = 620, 700
    draw.rounded_rectangle(
        (doc_x, doc_y, doc_x + doc_w, doc_y + doc_h),
        radius=48,
        fill=(255, 255, 255, 230),
    )

    # 文件折角
    fold = 110
    fold_pts = [
        (doc_x + doc_w - fold, doc_y),
        (doc_x + doc_w, doc_y + fold),
        (doc_x + doc_w - fold, doc_y + fold),
    ]
    draw.polygon(fold_pts, fill=(220, 230, 245, 200))

    # 文件上的橫線（模擬 CSV 行）
    line_color = (59, 130, 246, 100)
    for i in range(6):
        ly = doc_y + 180 + i * 70
        lx0 = doc_x + 70
        lx1 = doc_x + doc_w - 100 - (i % 3) * 50
        draw.rounded_rectangle(
            (lx0, ly, lx1, ly + 20),
            radius=10,
            fill=line_color,
        )

    # 轉換箭頭 — 右下角的圓形徽章
    badge_cx, badge_cy, badge_r = 760, 670, 160
    draw.ellipse(
        (badge_cx - badge_r, badge_cy - badge_r,
         badge_cx + badge_r, badge_cy + badge_r),
        fill=(16, 185, 129),  # 綠色
    )

    # 箭頭（兩個循環箭頭，簡化為 ⟳ 符號）
    arrow_color = (255, 255, 255)
    # 上箭頭
    cx, cy = badge_cx, badge_cy
    draw.polygon(
        [(cx - 35, cy - 30), (cx + 35, cy - 30), (cx, cy - 70)],
        fill=arrow_color,
    )
    draw.rounded_rectangle((cx - 12, cy - 30, cx + 12, cy + 50), radius=6, fill=arrow_color)
    draw.arc(
        (cx - 65, cy - 50, cx + 65, cy + 70),
        start=0, end=180,
        fill=arrow_color, width=22,
    )

    # 下箭頭
    draw.polygon(
        [(cx - 35, cy + 40), (cx + 35, cy + 40), (cx, cy + 80)],
        fill=arrow_color,
    )
    draw.rounded_rectangle((cx - 12, cy - 40, cx + 12, cy + 40), radius=6, fill=arrow_color)
    draw.arc(
        (cx - 65, cy - 60, cx + 65, cy + 60),
        start=180, end=360,
        fill=arrow_color, width=22,
    )

    # 底部文字 "XGB"
    try:
        font = ImageFont.truetype("/System/Library/Fonts/Helvetica.ttc", 90)
    except Exception:
        font = ImageFont.load_default()

    draw.text(
        (SIZE // 2, doc_y + doc_h + 85),
        "XGB",
        fill=(255, 255, 255),
        font=font,
        anchor="mm",
    )

    return img


def create_icns(img: Image.Image, output_path: Path):
    """從 1024x1024 圖片生成 macOS .icns。"""
    iconset = Path(tempfile.mkdtemp()) / "icon.iconset"
    iconset.mkdir()

    sizes = [16, 32, 64, 128, 256, 512]
    for s in sizes:
        img.resize((s, s), Image.LANCZOS).save(iconset / f"icon_{s}x{s}.png")
        s2 = s * 2
        if s2 <= 1024:
            img.resize((s2, s2), Image.LANCZOS).save(
                iconset / f"icon_{s}x{s}@2x.png"
            )
    img.save(iconset / "icon_512x512@2x.png")

    subprocess.run(
        ["iconutil", "-c", "icns", str(iconset), "-o", str(output_path)],
        check=True,
    )
    print(f"Generated: {output_path}")


if __name__ == "__main__":
    icon = make_icon()
    png_path = OUT_DIR / "xgb_icon.png"
    icon.save(png_path)
    print(f"PNG saved: {png_path}")

    icns_path = OUT_DIR / "xgb_icon.icns"
    create_icns(icon, icns_path)
