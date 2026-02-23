import fitz
doc = fitz.open(r'C:\Users\MJ\.clawdbot\media\inbound\039ca854-98c2-42b4-b899-6442a78c1908.pdf')

# Find Item 136 - large medical centre
for i, page in enumerate(doc):
    text = page.get_text()
    if 'Item 136' in text or 'large medical centre' in text.lower():
        print(f'--- PAGE {i+1} ---')
        print(text)
        print()
