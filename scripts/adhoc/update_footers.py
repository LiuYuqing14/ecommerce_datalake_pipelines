import os
import re
from datetime import datetime

# Regex to match the slogan (Anchor)
SLOGAN_REGEX = re.compile(
    r'<p align="center">\s*<sub>'
    r"✨ Transform the data. Tell the story. Build the future. ✨"
    r"</sub>\s*</p>",
    re.DOTALL | re.IGNORECASE,
)

# Regex to match the Nav block
NAV_REGEX = re.compile(
    r'<p align="center">\s*<a href="[^"]+">.*?🏠.*?</a>.*?</p>',
    re.DOTALL | re.IGNORECASE,
)

# Regex to match the Timestamp block
TIME_REGEX = re.compile(
    r'<p align="center">\s*<sub>Last updated:.*?</sub><br>', re.DOTALL | re.IGNORECASE
)


def get_link_prefix(file_path):
    """
    Returns the relative prefix to root from the file's directory.
    e.g. docs/resources/foo.md -> ../../
         README.md -> ""
    """
    dir_path = os.path.dirname(file_path)
    if not dir_path or dir_path == ".":
        return ""

    rel = os.path.relpath(".", dir_path)
    if rel == ".":
        return ""
    return rel + "/"


def construct_footer(file_path):
    prefix = get_link_prefix(file_path)
    readme_link = f"{prefix}README.md"
    hub_link = f"{prefix}RESOURCE_HUB.md"
    date_str = datetime.now().strftime("%Y-%m-%d")

    return f"""<p align=\"center\">
  <a href="{readme_link}">🏠 <b>Home</b></a>
  &nbsp;·&nbsp;
  <a href="{hub_link}">📚 <b>Resource Hub</b></a>
</p>

<p align="center">
  <sub>Last updated: {date_str}</sub><br>
  <sub>✨ Transform the data. Tell the story. Build the future. ✨</sub>
</p>
"""


def update_file(file_path):
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            content = f.read()
    except Exception as e:
        print(f"Skipping {file_path}: {e}")
        return

    slogan_match = SLOGAN_REGEX.search(content)
    new_footer = construct_footer(file_path)

    if slogan_match:
        # We found the slogan.
        # Now we search backwards to see if there is an existing timestamp
        # and/or nav block immediately preceding it.
        # We define "immediately preceding" as "only whitespace in between".

        start_cut = slogan_match.start()

        # 1. Look for Timestamp immediately before start_cut
        # We scan the text before start_cut
        pre_text = content[:start_cut]

        # Helper to strip trailing whitespace from a string and return length stripped
        def trailing_ws_len(s):
            return len(s) - len(s.rstrip())

        # Consume whitespace before slogan
        ws_len = trailing_ws_len(pre_text)
        cursor = start_cut - ws_len

        # Check for Timestamp
        # We look for the regex match ending at cursor
        # Since regex searches from start, we'll try to find all matches
        # and check the last one

        # Optimization: verify if the end of content[:cursor] matches the pattern
        # This is tricky with regex.
        # Instead, let's grab a chunk of text ending at cursor
        chunk_size = 2000
        chunk_start = max(0, cursor - chunk_size)
        chunk = content[chunk_start:cursor]

        # Find matches in chunk
        ts_matches = list(TIME_REGEX.finditer(chunk))
        if ts_matches:
            last_ts = ts_matches[-1]
            if last_ts.end() == len(chunk):
                # Found valid timestamp block adjacent to slogan
                cursor = chunk_start + last_ts.start()
                # Consume whitespace before timestamp
                cursor -= trailing_ws_len(content[:cursor])

        # Refresh chunk for Nav check
        chunk_start = max(0, cursor - chunk_size)
        chunk = content[chunk_start:cursor]

        nav_matches = list(NAV_REGEX.finditer(chunk))
        if nav_matches:
            last_nav = nav_matches[-1]
            if last_nav.end() == len(chunk):
                # Found valid nav block adjacent
                cursor = chunk_start + last_nav.start()

        # Now we replace from cursor to end_cut with new footer
        # Maintain some separation from previous content
        clean_pre = content[:cursor].rstrip()

        # Check if we are appending to empty file (unlikely) or just need newlines
        if not clean_pre:
            final_content = new_footer
        else:
            # Standard markdown spacing: Empty line + separator + Empty line
            # But the separator '---' might be part of the doc body or the footer.
            # Let's check if the doc has a separator at the end of clean_pre
            # If so, keep it. If not, add one?
            # Users often put '---' before the footer manually.
            # Let's standardize: Ensure there is a '---' before the footer
            # if the file is long enough.

            if not clean_pre.endswith("---") and len(clean_pre) > 10:
                final_content = f"{clean_pre}\n\n---\n\n{new_footer}"
            else:
                final_content = f"{clean_pre}\n\n{new_footer}"

    else:
        # No slogan found. Append to end.
        clean_pre = content.rstrip()
        final_content = f"{clean_pre}\n\n---\n\n{new_footer}"

    if content != final_content:
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(final_content)
        print(f"Updated: {file_path}")
    else:
        print(f"No change: {file_path}")


def main():
    ignore_dirs = {
        ".venv",
        ".git",
        "node_modules",
        "dbt_packages",
        "__pycache__",
        ".mypy_cache",
        ".ruff_cache",
        "coverage_html",
        ".claude",
        ".gcloud",
        ".github",
        ".vscode",
        "target",
        "logs",
        "plugins",
    }

    for root, dirs, files in os.walk("."):
        # Modify dirs in-place to prune walk
        dirs[:] = [d for d in dirs if d not in ignore_dirs]

        for file in files:
            if file.endswith(".md"):
                update_file(os.path.join(root, file))


if __name__ == "__main__":
    main()
