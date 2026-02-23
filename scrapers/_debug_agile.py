import re

text = """If you are interested in any of the Pharmacies listed here, please fill in the CONFIDENTIALITY AGREEMENT FORM BELOW

All information obtained is STRICTLY CONFIDENTIAL.

NEW LISTINGS:   

Ref PU:  Mid North Coast City Pharmacy. Solid, dependable but with potential.

ON HOLD Ref PL: Larger Mid North Coast branded Pharmacy.

To find out more about PL and PU, fill in the Confidentiality Agreement below and we will send you the password to access the information

Ref AHH:  UNDER OFFER Fantastic suburban style Pharmacy in a large inland city.

VICTORIAN PHARMACIES

Ref LL: Small but massive potential. 1.5 hrs from Melbourne CBD MMM5

Ref LP: Solid and Profitable MMM5 and available with freehold!

To find out more about LL and LP, fill in the Confidentiality Agreement below and we will send you the password to access the information

SYDNEY & NEWCASTLE PHARMACIES:

 Ref LN: Iconic:- In Sydney's Eastern Suburbs, this Pharmacy could be anything. Solidly profitable now, but this is all opportunity!

 Ref PP:  NEW LISTING: Remarkable Pharmacy in an unusual but fantastic location. North Sydney area.

NSW REGIONAL PHARMACIES:

 Ref LT:  This will be popular - A medium sized Pharmacy in the Central West of NSW with a large increase in RPMA funding in the 8CPA. MMM3"""

NAV_WORDS = {'ABOUT', 'BLOG', 'CONTACT', 'SERVICE', 'SERVICES', 'HOME',
             'SELL', 'BUY', 'SOLD', 'TESTIMONIALS', 'PHARMACY', 'PHARMACIES',
             'CONFIDENTIALITY', 'AGREEMENT', 'FORM', 'BELOW', 'VICTORIAN',
             'SYDNEY', 'NEWCASTLE', 'REGIONAL', 'NEW', 'LISTINGS'}

VALID_REF = re.compile(r'^[A-Z]{2,4}$')

pattern = re.compile(
    r'Ref\s+([A-Z]{2,4}):\s*(?:UNDER OFFER\s*)?(?:NEW LISTING:?\s*)?(.+)',
    re.IGNORECASE | re.MULTILINE
)

for match in pattern.finditer(text):
    ref = match.group(1).strip().upper()
    desc = match.group(2).strip()
    desc = re.sub(r'[\u200b\u200c\u200d\ufeff]', '', desc).strip()
    
    skip_reason = None
    if not VALID_REF.match(ref):
        skip_reason = "invalid ref"
    elif ref in NAV_WORDS:
        skip_reason = "nav word"
    elif len(desc) < 10:
        skip_reason = f"too short ({len(desc)})"
    elif any(nav in desc.upper() for nav in ['CLICK', 'FILL IN', 'PASSWORD', 'AGREEMENT', 'CONFIDENTIALITY']):
        skip_reason = "nav content"
    
    status = "SKIP" if skip_reason else "OK"
    print(f"  [{status:4s}] Ref {ref}: {desc[:80]}")
    if skip_reason:
        print(f"         reason: {skip_reason}")
