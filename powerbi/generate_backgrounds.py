"""
Generates 4 dark dashboard background PNGs for Power BI pages.
Output: powerbi/backgrounds/bg_*.png  (1280x720)
"""
from PIL import Image, ImageDraw, ImageFilter, ImageFont
import numpy as np
from pathlib import Path

OUT = Path(__file__).parent / "backgrounds"
OUT.mkdir(exist_ok=True)

W, H = 1280, 720

# ── Color palette ─────────────────────────────────────────────────────────────
DARK_BASE   = (13,  21,  32)
DARK_CARD   = (10,  24,  40)
BLUE        = (0,  196, 255)
YELLOW      = (255, 209, 102)
TEAL        = (6,  214, 160)
PURPLE      = (131, 56, 236)

PAGES = [
    ("bg_payroll_overview",    "PAYROLL OVERVIEW",    BLUE,   YELLOW),
    ("bg_employee_breakdown",  "EMPLOYEE BREAKDOWN",  YELLOW, BLUE),
    ("bg_geographic_grade",    "GEOGRAPHIC & GRADE",  TEAL,   BLUE),
    ("bg_indemnity_analysis",  "INDEMNITY ANALYSIS",  BLUE,   PURPLE),
]


def rgba(color, alpha=255):
    return (*color, alpha)


def make_gradient_array(w, h, c1, c2, c3, angle=135):
    """3-stop linear gradient as numpy array (RGBA)."""
    arr = np.zeros((h, w, 4), dtype=np.uint8)
    if angle == 135:
        t = (np.arange(h)[:, None] / h + np.arange(w)[None, :] / w) / 2
    else:
        t = np.arange(h)[:, None] / h * np.ones((1, w))
    t = np.clip(t, 0, 1)
    mask1 = t < 0.5
    mask2 = t >= 0.5
    t1 = t * 2
    t2 = (t - 0.5) * 2
    for ch in range(3):
        arr[:, :, ch] = np.where(mask1,
            c1[ch] + (c2[ch] - c1[ch]) * t1,
            c2[ch] + (c3[ch] - c2[ch]) * t2
        ).astype(np.uint8)
    arr[:, :, 3] = 255
    return arr


def glow_circle(size, color, alpha_center=180):
    """Soft radial glow circle."""
    img = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    arr = np.zeros((size, size, 4), dtype=np.float32)
    cx, cy = size / 2, size / 2
    ys, xs = np.ogrid[:size, :size]
    dist = np.sqrt((xs - cx)**2 + (ys - cy)**2)
    alpha = np.clip(1 - dist / (size / 2), 0, 1) ** 1.8
    for ch in range(3):
        arr[:, :, ch] = color[ch]
    arr[:, :, 3] = alpha * alpha_center
    img = Image.fromarray(arr.astype(np.uint8), "RGBA")
    return img.filter(ImageFilter.GaussianBlur(radius=size // 5))


def dot_grid(w, h, spacing=32, dot_r=1, color=(255, 255, 255), alpha=18):
    """Subtle dot grid overlay."""
    img = Image.new("RGBA", (w, h), (0, 0, 0, 0))
    d = ImageDraw.Draw(img)
    for y in range(spacing, h, spacing):
        for x in range(spacing, w, spacing):
            d.ellipse([x - dot_r, y - dot_r, x + dot_r, y + dot_r],
                      fill=(*color, alpha))
    return img


def horizontal_line(w, color, alpha=80, thickness=1):
    img = Image.new("RGBA", (w, thickness), (0, 0, 0, 0))
    d = ImageDraw.Draw(img)
    d.line([(0, 0), (w, 0)], fill=(*color, alpha), width=thickness)
    return img


def left_accent_bar(h, c_top, c_bottom, width=4):
    img = Image.new("RGBA", (width, h), (0, 0, 0, 0))
    arr = np.zeros((h, width, 4), dtype=np.float32)
    for y in range(h):
        t = y / h
        if t < 0.5:
            a = t * 2
            clr = [c_top[ch] * a for ch in range(3)]
            alpha = a * 200
        else:
            a = (t - 0.5) * 2
            clr = [c_top[ch] * (1 - a) + c_bottom[ch] * a for ch in range(3)]
            alpha = (1 - a * 0.5) * 200
        for ch in range(3):
            arr[y, :, ch] = clr[ch]
        arr[y, :, 3] = alpha
    return Image.fromarray(arr.astype(np.uint8), "RGBA")


def make_background(filename, title, accent, glow2):
    # ── Base ──────────────────────────────────────────────────────────────────
    grad = make_gradient_array(W, H, DARK_BASE, DARK_CARD, DARK_BASE)
    base = Image.fromarray(grad, "RGBA")

    # ── Dot grid ──────────────────────────────────────────────────────────────
    dots = dot_grid(W, H)
    base = Image.alpha_composite(base, dots)

    # ── Glow top-right ────────────────────────────────────────────────────────
    g1 = glow_circle(420, accent, alpha_center=90)
    base.paste(g1, (W - 280, -160), g1)

    # ── Glow bottom-left ─────────────────────────────────────────────────────
    g2 = glow_circle(320, glow2, alpha_center=70)
    base.paste(g2, (-120, H - 220), g2)

    # ── Glow mid subtle ──────────────────────────────────────────────────────
    g3 = glow_circle(200, accent, alpha_center=30)
    base.paste(g3, (W // 2 - 100, H // 2 - 100), g3)

    # ── Top header bar ────────────────────────────────────────────────────────
    header = Image.new("RGBA", (W, 58), (*DARK_CARD, 220))
    base.paste(header, (0, 0), header)

    # accent line under header
    line = horizontal_line(W, accent, alpha=120, thickness=2)
    base.paste(line, (0, 58), line)

    # ── Left accent bar ───────────────────────────────────────────────────────
    bar = left_accent_bar(H, accent, glow2, width=4)
    base.paste(bar, (0, 0), bar)

    # ── Bottom accent line ────────────────────────────────────────────────────
    bline = horizontal_line(W, glow2, alpha=60, thickness=2)
    base.paste(bline, (0, H - 2), bline)

    # ── Title text ────────────────────────────────────────────────────────────
    draw = ImageDraw.Draw(base)
    try:
        font_title = ImageFont.truetype("C:/Windows/Fonts/segoeuib.ttf", 14)
        font_sub   = ImageFont.truetype("C:/Windows/Fonts/segoeui.ttf",  10)
    except Exception:
        font_title = ImageFont.load_default()
        font_sub   = font_title

    # letter-spaced title simulation — draw char by char
    x, y = 24, 20
    spaced_title = "  ".join(title)
    draw.text((x, y), spaced_title, font=font_title, fill=(*accent, 230))

    # thin yellow separator line after title
    tw = draw.textlength(spaced_title, font=font_title)
    draw.line([(x + tw + 12, 29), (x + tw + 80, 29)],
              fill=(*YELLOW, 160), width=1)

    # subtitle
    draw.text((x, 38), "INSAF  ·  PAYROLL INTELLIGENCE PLATFORM",
              font=font_sub, fill=(138, 155, 174, 180))

    # ── Corner decoration (top-left geometric) ───────────────────────────────
    draw.rectangle([0, 0, 3, 58], fill=(*accent, 255))

    # small accent squares top-right
    sq = 6
    for i, color in enumerate([accent, YELLOW, glow2]):
        draw.rectangle([W - 20 - i * 12, 8, W - 14 - i * 12, 8 + sq],
                       fill=(*color, 180))

    # ── Convert to RGB and save ───────────────────────────────────────────────
    final = Image.new("RGB", (W, H), DARK_BASE)
    final.paste(base, mask=base.split()[3])
    path = OUT / f"{filename}.png"
    final.save(path, "PNG", optimize=True)
    print(f"  Saved: {path}")


if __name__ == "__main__":
    print("Generating backgrounds...")
    for fname, title, accent, glow2 in PAGES:
        make_background(fname, title, accent, glow2)
    print("Done. Import the PNGs from powerbi/backgrounds/ into Power BI.")
    print("Format page → Canvas background → Image → transparency 0%")
