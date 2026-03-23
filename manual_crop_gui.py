
#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from pathlib import Path
import tkinter as tk
from tkinter import messagebox
from typing import Optional

from PIL import Image, ImageTk

VALID_EXTS = {".png", ".jpg", ".jpeg", ".tif", ".tiff", ".bmp", ".webp"}


@dataclass
class CropRecord:
    filename: str
    crop_x1: int
    crop_y1: int
    crop_x2: int
    crop_y2: int
    status: str


class ManualCropApp:
    def __init__(self, root: tk.Tk, input_dir: Path, output_dir: Path, csv_path: Path):
        self.root = root
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.csv_path = csv_path
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.csv_path.parent.mkdir(parents=True, exist_ok=True)

        self.files = sorted(
            [p for p in self.input_dir.iterdir() if p.is_file() and p.suffix.lower() in VALID_EXTS]
        )
        if not self.files:
            raise SystemExit(f"No images found in {self.input_dir}")

        self.index = 0
        self.records: dict[str, CropRecord] = self._load_existing_records()

        self.current_image: Optional[Image.Image] = None
        self.tk_img = None
        self.scale = 1.0
        self.display_w = 0
        self.display_h = 0

        self.start_x = None
        self.start_y = None
        self.rect_id = None
        self.current_crop = None  # original image coords

        self._build_ui()
        self._bind_keys()
        self._load_current()

    def _build_ui(self):
        self.root.title("Manual Crop Tool")
        self.root.geometry("1400x950")

        top = tk.Frame(self.root)
        top.pack(side=tk.TOP, fill=tk.X, padx=6, pady=6)

        self.info_var = tk.StringVar(value="")
        tk.Label(top, textvariable=self.info_var, anchor="w", justify="left").pack(side=tk.LEFT, padx=4)

        button_bar = tk.Frame(self.root)
        button_bar.pack(side=tk.TOP, fill=tk.X, padx=6, pady=4)

        tk.Button(button_bar, text="Prev", command=self.prev_image, width=10).pack(side=tk.LEFT, padx=4)
        tk.Button(button_bar, text="Next", command=self.next_image, width=10).pack(side=tk.LEFT, padx=4)
        tk.Button(button_bar, text="Reset Box", command=self.reset_crop, width=12).pack(side=tk.LEFT, padx=4)
        tk.Button(button_bar, text="Save Crop", command=self.save_crop, width=12).pack(side=tk.LEFT, padx=4)
        tk.Button(button_bar, text="Mark Skip", command=self.mark_skip, width=12).pack(side=tk.LEFT, padx=4)
        tk.Button(button_bar, text="Save CSV", command=self.save_csv, width=12).pack(side=tk.LEFT, padx=4)
        tk.Button(button_bar, text="Quit", command=self.on_quit, width=10).pack(side=tk.RIGHT, padx=4)

        help_text = (
            "Drag a box over the image. Shortcuts: "
            "A/D=prev/next, R=reset, S=save crop, K=mark skip, Ctrl+S=save CSV, Q=quit"
        )
        tk.Label(self.root, text=help_text, anchor="w").pack(side=tk.TOP, fill=tk.X, padx=8, pady=(0, 4))

        self.canvas = tk.Canvas(self.root, bg="gray20", cursor="cross")
        self.canvas.pack(side=tk.TOP, fill=tk.BOTH, expand=True, padx=6, pady=6)

        self.canvas.bind("<ButtonPress-1>", self.on_press)
        self.canvas.bind("<B1-Motion>", self.on_drag)
        self.canvas.bind("<ButtonRelease-1>", self.on_release)
        self.root.bind("<Configure>", self._debounced_redraw)

    def _bind_keys(self):
        self.root.bind("a", lambda e: self.prev_image())
        self.root.bind("d", lambda e: self.next_image())
        self.root.bind("r", lambda e: self.reset_crop())
        self.root.bind("s", lambda e: self.save_crop())
        self.root.bind("k", lambda e: self.mark_skip())
        self.root.bind("q", lambda e: self.on_quit())
        self.root.bind("<Control-s>", lambda e: self.save_csv())

    def _debounced_redraw(self, event):
        if event.widget == self.root:
            self.root.after(60, self._redraw_current)

    def _load_existing_records(self) -> dict[str, CropRecord]:
        records = {}
        if not self.csv_path.exists():
            return records
        with self.csv_path.open("r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    records[row["filename"]] = CropRecord(
                        filename=row["filename"],
                        crop_x1=int(row["crop_x1"]),
                        crop_y1=int(row["crop_y1"]),
                        crop_x2=int(row["crop_x2"]),
                        crop_y2=int(row["crop_y2"]),
                        status=row.get("status", "crop"),
                    )
                except Exception:
                    continue
        return records

    def _load_current(self):
        path = self.files[self.index]
        self.current_image = Image.open(path)
        self.current_crop = None

        rec = self.records.get(path.name)
        if rec and rec.status == "crop":
            self.current_crop = (rec.crop_x1, rec.crop_y1, rec.crop_x2, rec.crop_y2)

        self._redraw_current()

    def _redraw_current(self):
        if self.current_image is None:
            return

        self.canvas.delete("all")
        canvas_w = max(self.canvas.winfo_width(), 300)
        canvas_h = max(self.canvas.winfo_height(), 300)

        img_w, img_h = self.current_image.size
        self.scale = min(canvas_w / img_w, canvas_h / img_h, 1.0)
        self.display_w = max(1, int(img_w * self.scale))
        self.display_h = max(1, int(img_h * self.scale))

        resized = self.current_image.resize((self.display_w, self.display_h), Image.Resampling.LANCZOS)
        self.tk_img = ImageTk.PhotoImage(resized)

        x0 = (canvas_w - self.display_w) // 2
        y0 = (canvas_h - self.display_h) // 2

        self.image_x0 = x0
        self.image_y0 = y0

        self.canvas.create_image(x0, y0, anchor="nw", image=self.tk_img)

        path = self.files[self.index]
        rec = self.records.get(path.name)
        status = rec.status if rec else "unreviewed"
        self.info_var.set(
            f"{self.index + 1}/{len(self.files)}   {path.name}   "
            f"orig={img_w}x{img_h}   display={self.display_w}x{self.display_h}   status={status}"
        )

        if self.current_crop:
            x1, y1, x2, y2 = self.current_crop
            dx1, dy1 = self.orig_to_display(x1, y1)
            dx2, dy2 = self.orig_to_display(x2, y2)
            self.rect_id = self.canvas.create_rectangle(dx1, dy1, dx2, dy2, outline="red", width=2)

    def display_to_orig(self, x: int, y: int) -> tuple[int, int]:
        rel_x = min(max(x - self.image_x0, 0), self.display_w)
        rel_y = min(max(y - self.image_y0, 0), self.display_h)
        ox = int(rel_x / self.scale)
        oy = int(rel_y / self.scale)
        ox = min(max(ox, 0), self.current_image.size[0] - 1)
        oy = min(max(oy, 0), self.current_image.size[1] - 1)
        return ox, oy

    def orig_to_display(self, x: int, y: int) -> tuple[int, int]:
        dx = int(x * self.scale) + self.image_x0
        dy = int(y * self.scale) + self.image_y0
        return dx, dy

    def on_press(self, event):
        if self.current_image is None:
            return
        self.start_x = event.x
        self.start_y = event.y
        if self.rect_id:
            self.canvas.delete(self.rect_id)
            self.rect_id = None

    def on_drag(self, event):
        if self.start_x is None or self.start_y is None:
            return
        if self.rect_id:
            self.canvas.delete(self.rect_id)
        self.rect_id = self.canvas.create_rectangle(
            self.start_x, self.start_y, event.x, event.y, outline="red", width=2
        )

    def on_release(self, event):
        if self.start_x is None or self.start_y is None:
            return
        ox1, oy1 = self.display_to_orig(self.start_x, self.start_y)
        ox2, oy2 = self.display_to_orig(event.x, event.y)
        x1, x2 = sorted((ox1, ox2))
        y1, y2 = sorted((oy1, oy2))
        if abs(x2 - x1) < 10 or abs(y2 - y1) < 10:
            self.current_crop = None
            if self.rect_id:
                self.canvas.delete(self.rect_id)
                self.rect_id = None
        else:
            self.current_crop = (x1, y1, x2, y2)
        self.start_x = None
        self.start_y = None

    def reset_crop(self):
        self.current_crop = None
        if self.rect_id:
            self.canvas.delete(self.rect_id)
            self.rect_id = None

    def save_crop(self):
        path = self.files[self.index]
        if not self.current_crop:
            messagebox.showwarning("No crop", "Draw a crop box first.")
            return
        x1, y1, x2, y2 = self.current_crop
        cropped = self.current_image.crop((x1, y1, x2, y2))
        out_path = self.output_dir / path.name
        cropped.save(out_path)

        self.records[path.name] = CropRecord(
            filename=path.name,
            crop_x1=x1,
            crop_y1=y1,
            crop_x2=x2,
            crop_y2=y2,
            status="crop",
        )
        self.save_csv(show_message=False)
        messagebox.showinfo("Saved", f"Saved crop to:\n{out_path}")

    def mark_skip(self):
        path = self.files[self.index]
        self.records[path.name] = CropRecord(
            filename=path.name,
            crop_x1=0,
            crop_y1=0,
            crop_x2=0,
            crop_y2=0,
            status="skip",
        )
        self.save_csv(show_message=False)
        self.next_image()

    def prev_image(self):
        if self.index > 0:
            self.index -= 1
            self._load_current()

    def next_image(self):
        if self.index < len(self.files) - 1:
            self.index += 1
            self._load_current()

    def save_csv(self, show_message: bool = True):
        fieldnames = ["filename", "crop_x1", "crop_y1", "crop_x2", "crop_y2", "status"]
        with self.csv_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for path in self.files:
                rec = self.records.get(path.name)
                if rec is None:
                    writer.writerow({
                        "filename": path.name,
                        "crop_x1": "",
                        "crop_y1": "",
                        "crop_x2": "",
                        "crop_y2": "",
                        "status": "",
                    })
                else:
                    writer.writerow({
                        "filename": rec.filename,
                        "crop_x1": rec.crop_x1,
                        "crop_y1": rec.crop_y1,
                        "crop_x2": rec.crop_x2,
                        "crop_y2": rec.crop_y2,
                        "status": rec.status,
                    })
        if show_message:
            messagebox.showinfo("Saved", f"Saved review CSV:\n{self.csv_path}")

    def on_quit(self):
        self.save_csv(show_message=False)
        self.root.destroy()


def main():
    parser = argparse.ArgumentParser(description="Manual crop GUI for preprocessed images.")
    parser.add_argument("--input", default="out/preprocessed", help="Input image directory")
    parser.add_argument("--output", default="out/manual_crops", help="Directory to save cropped images")
    parser.add_argument("--csv", default="out/manual_crops/review.csv", help="CSV review log")
    args = parser.parse_args()

    root = tk.Tk()
    app = ManualCropApp(root, Path(args.input), Path(args.output), Path(args.csv))
    root.mainloop()


if __name__ == "__main__":
    main()
