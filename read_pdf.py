import fitz
doc = fitz.open(r'C:\Users\MJ\.clawdbot\media\inbound\039ca854-98c2-42b4-b899-6442a78c1908.pdf')
print(f'Pages: {len(doc)}')
title = doc.metadata.get("title", "?")
print(f'Title: {title}')
print()
for i, page in enumerate(doc):
    text = page.get_text()
    if i < 8:
        print(f'--- PAGE {i+1} ---')
        print(text[:3000])
        print()
