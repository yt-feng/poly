from __future__ import annotations

import json
import re
from copy import deepcopy
from pathlib import Path
from typing import Any, Dict, List, Tuple

import fitz

META_LABEL_PATTERNS = [
    "mckinsey-style",
    "bcg-style",
    "consulting-style",
    "sample card",
    "制作说明",
    "样例图卡",
]


def run_pdf_qa(pdf_path: Path, html_path: Path, qa_dir: Path, *, render_pages: int = 10) -> Dict[str, Any]:
    qa_dir.mkdir(parents=True, exist_ok=True)
    result: Dict[str, Any] = {
        "passed": True,
        "pdf_path": str(pdf_path),
        "html_path": str(html_path),
        "checks": [],
        "issues": [],
        "recommendations": [],
        "page_count": 0,
        "screenshots": [],
    }

    if not pdf_path.exists() or pdf_path.stat().st_size < 2048:
        _fail(result, "missing_or_tiny_pdf", "PDF is missing or suspiciously small.", "rerender_pdf")
        _write(result, qa_dir)
        return result

    try:
        doc = fitz.open(str(pdf_path))
    except Exception as exc:
        _fail(result, "pdf_open_error", f"Unable to open PDF: {exc}", "rerender_pdf")
        _write(result, qa_dir)
        return result

    result["page_count"] = doc.page_count
    if doc.page_count < 3:
        _fail(result, "too_few_pages", "PDF has fewer than three pages.", "check_renderer")
    else:
        _pass(result, "page_count", f"PDF has {doc.page_count} pages.")

    full_text = ""
    for page_index in range(doc.page_count):
        page = doc.load_page(page_index)
        full_text += "\n" + page.get_text("text")
        for issue in _inspect_page_text_layout(page, page_index + 1):
            _fail(result, issue["code"], issue["message"], issue["recommendation"])

        if page_index < render_pages:
            pix = page.get_pixmap(matrix=fitz.Matrix(1.4, 1.4), alpha=False)
            out = qa_dir / f"page_{page_index + 1:03d}.png"
            pix.save(str(out))
            result["screenshots"].append(str(out))

    if len(full_text.strip()) < 500:
        _fail(result, "low_text_extractability", "Extracted PDF text is very short; text may be rendered incorrectly.", "check_pdf_text")
    else:
        _pass(result, "text_extractability", "PDF text extraction looks healthy.")

    lower = full_text.lower()
    for pattern in META_LABEL_PATTERNS:
        if pattern.lower() in lower:
            _fail(result, "meta_label_visible", f"Visible meta label found: {pattern}", "strip_meta_labels")

    html = html_path.read_text(encoding="utf-8", errors="ignore") if html_path.exists() else ""
    for risk in _inspect_html_page_density(html):
        _fail(result, risk["code"], risk["message"], risk["recommendation"])

    result["passed"] = not result["issues"]
    _write(result, qa_dir)
    return result


def apply_pdf_qa_fixes(report: Dict[str, Any], qa_result: Dict[str, Any]) -> Dict[str, Any]:
    fixed = deepcopy(report)
    fixed["_layout_profile"] = "compact"
    fixed["_qa_autofixed"] = True
    fixed["_qa_recommendations"] = qa_result.get("recommendations", [])

    fixed["report_title"] = _clean_text(fixed.get("report_title", ""), 70)
    fixed["report_subtitle"] = _clean_text(fixed.get("report_subtitle", ""), 130)
    fixed["executive_summary"] = [_clean_text(x, 115) for x in fixed.get("executive_summary", [])[:6]]

    for step in fixed.get("method_steps", []):
        step["name"] = _clean_text(step.get("name", ""), 42)
        step["description"] = _clean_text(step.get("description", ""), 95)

    for section in fixed.get("sections", []):
        section["title"] = _clean_text(section.get("title", ""), 72)
        section["lead"] = _clean_text(section.get("lead", ""), 120)
        section["paragraphs"] = [_clean_text(p, 260) for p in section.get("paragraphs", [])[:3]]
        section["key_takeaways"] = [_clean_text(x, 95) for x in section.get("key_takeaways", [])[:2]]

    for card in fixed.get("insight_cards", []):
        card["title"] = _clean_text(card.get("title", ""), 58)
        card["subtitle"] = _clean_text(card.get("subtitle", ""), 82)
        card["bullets"] = [_clean_text(x, 52) for x in card.get("bullets", [])[:3]]
        card["highlight_label"] = _clean_text(card.get("highlight_label", ""), 20)
        card["exhibit_label"] = _clean_text(card.get("exhibit_label", ""), 28)

    for chart in fixed.get("charts", []):
        chart["title"] = _clean_text(chart.get("title", ""), 64)
        chart["subtitle"] = _clean_text(chart.get("subtitle", ""), 95)
        chart["categories"] = [_clean_text(str(x), 18) for x in chart.get("categories", [])[:8]]
        chart["caption"] = _clean_text(chart.get("caption", ""), 115)
        chart["source_note"] = _clean_text(chart.get("source_note", ""), 110)
        for item in chart.get("series", []):
            item["name"] = _clean_text(item.get("name", ""), 24)
            values = item.get("values", [])
            if len(values) > len(chart.get("categories", [])):
                item["values"] = values[: len(chart.get("categories", []))]

    return fixed


def _inspect_page_text_layout(page: fitz.Page, page_no: int) -> List[Dict[str, str]]:
    issues: List[Dict[str, str]] = []
    blocks = []
    for block in page.get_text("blocks"):
        if len(block) < 5:
            continue
        x0, y0, x1, y1, text = block[:5]
        text = str(text).strip()
        if len(text) < 4:
            continue
        blocks.append((x0, y0, x1, y1, text))

    for i in range(len(blocks)):
        for j in range(i + 1, len(blocks)):
            a = blocks[i]
            b = blocks[j]
            overlap = _overlap_ratio(a[:4], b[:4])
            if overlap > 0.16 and not _same_line_artifact(a, b):
                issues.append({
                    "code": "text_block_overlap",
                    "message": f"Page {page_no}: possible text overlap between '{a[4][:28]}' and '{b[4][:28]}'.",
                    "recommendation": "compact_layout_and_truncate",
                })
                return issues

    text_dict = page.get_text("dict")
    sizes = []
    for block in text_dict.get("blocks", []):
        for line in block.get("lines", []):
            for span in line.get("spans", []):
                txt = span.get("text", "").strip()
                size = float(span.get("size", 0))
                if txt and size:
                    sizes.append(size)
    if sizes:
        if min(sizes) < 4.5:
            issues.append({"code": "font_too_small", "message": f"Page {page_no}: text below 4.5pt detected.", "recommendation": "increase_min_font_or_reduce_content"})
        if max(sizes) > 52:
            issues.append({"code": "font_too_large", "message": f"Page {page_no}: unusually large text detected.", "recommendation": "reduce_cover_or_title_font"})
    return issues


def _inspect_html_page_density(html: str) -> List[Dict[str, str]]:
    risks = []
    pages = re.findall(r"<section class='page[^']*'.*?</section>", html, flags=re.DOTALL)
    for idx, page in enumerate(pages, start=1):
        text = re.sub(r"<[^>]+>", " ", page)
        text_len = len(re.sub(r"\s+", "", text))
        visuals = page.count("<img")
        if text_len > 1800 and visuals >= 1:
            risks.append({"code": "dense_page_with_visual", "message": f"HTML page {idx}: dense text plus visual may overflow.", "recommendation": "compact_layout_and_truncate"})
        elif text_len > 2600:
            risks.append({"code": "dense_text_page", "message": f"HTML page {idx}: text density is high.", "recommendation": "truncate_section_text"})
    return risks


def _clean_text(value: Any, max_chars: int) -> str:
    text = str(value or "")
    for pattern in META_LABEL_PATTERNS:
        text = re.sub(re.escape(pattern), "", text, flags=re.IGNORECASE)
    text = re.sub(r"\s+", " ", text).strip()
    if len(text) <= max_chars:
        return text
    return text[: max_chars - 3].rstrip() + "..."


def _overlap_ratio(a: Tuple[float, float, float, float], b: Tuple[float, float, float, float]) -> float:
    ax0, ay0, ax1, ay1 = a
    bx0, by0, bx1, by1 = b
    ix0, iy0 = max(ax0, bx0), max(ay0, by0)
    ix1, iy1 = min(ax1, bx1), min(ay1, by1)
    if ix1 <= ix0 or iy1 <= iy0:
        return 0.0
    inter = (ix1 - ix0) * (iy1 - iy0)
    area_a = max(1.0, (ax1 - ax0) * (ay1 - ay0))
    area_b = max(1.0, (bx1 - bx0) * (by1 - by0))
    return inter / min(area_a, area_b)


def _same_line_artifact(a: Tuple, b: Tuple) -> bool:
    return abs(a[1] - b[1]) < 2.0 and abs(a[3] - b[3]) < 2.0


def _fail(result: Dict[str, Any], code: str, message: str, recommendation: str) -> None:
    result["issues"].append({"code": code, "message": message, "recommendation": recommendation})
    if recommendation not in result["recommendations"]:
        result["recommendations"].append(recommendation)


def _pass(result: Dict[str, Any], code: str, message: str) -> None:
    result["checks"].append({"code": code, "message": message})


def _write(result: Dict[str, Any], qa_dir: Path) -> None:
    (qa_dir / "pdf_qa.json").write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
