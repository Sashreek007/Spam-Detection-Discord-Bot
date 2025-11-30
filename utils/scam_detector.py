from transformers import pipeline
import re
from typing import Tuple
from config import Config


class ScamDetector:
    def __init__(self):
        """Initialize the scam detection model."""
        print(f"Loading model: {Config.MODEL_NAME}")
        self.classifier = pipeline(
            "text-classification",
            model=Config.MODEL_NAME,
            device=-1  # Use CPU, change to 0 for GPU
        )
        print("Model loaded successfully!")

    def detect(self, text: str) -> Tuple[bool, float, str]:
        """
        Detect if a message is a scam.

        Args:
            text: Message content to analyze

        Returns:
            Tuple of (is_scam, confidence_score, reason)
        """
        if not text or len(text.strip()) == 0:
            return False, 0.0, "Empty message"

        cleaned_text = text

        # Remove mention markers and IDs, replace with placeholder text
        cleaned_text = re.sub(r'<@!?\d+>', 'user', cleaned_text)
        cleaned_text = re.sub(r'<@&\d+>', 'role', cleaned_text)
        cleaned_text = cleaned_text.replace('@everyone', 'everyone')
        cleaned_text = cleaned_text.replace('@here', 'here')
        cleaned_text = re.sub(r'@\w+', '', cleaned_text)  # Remove other @mentions
        cleaned_text = cleaned_text.strip()

        # If message is empty after cleaning, skip it
        if not cleaned_text or len(cleaned_text.strip()) == 0:
            return False, 0.0, "Empty message"

        # Check for suspicious patterns (Discord-specific giveaway scams)
        has_suspicious = self._check_suspicious_patterns(cleaned_text)

        # Run ML model on cleaned text
        result = self.classifier(cleaned_text)[0]
        score = result['score']
        label = result['label']

        # Map numeric labels to text
        label_map = {
            'LABEL_0': 'HAM',   # Legitimate message
            'LABEL_1': 'SPAM'   # Spam/Scam message
        }
        label = label_map.get(label, label).upper()

        # DEBUG OUTPUT
        print(f"\n[DETECTOR] Original: {text[:80]}")
        print(f"[DETECTOR] Cleaned: {cleaned_text[:80]}")
        print(f"[DETECTOR] Label: {label} | Score: {score:.4f} | Suspicious: {has_suspicious}")
        print(f"[DETECTOR] Threshold: {Config.SCAM_THRESHOLD}")
        print(f"[DETECTOR] Check 1 (label=SPAM AND score > threshold): {label == 'SPAM'} and {score > Config.SCAM_THRESHOLD}")
        print(f"[DETECTOR] Check 2 (has suspicious patterns): {has_suspicious}\n")

        # Determine if it's a scam
        is_scam = False
        reason = ""

        # Check 1: Model says SPAM and score above threshold
        if label == 'SPAM' and score > Config.SCAM_THRESHOLD:
            is_scam = True
            reason = f"ML Detection ({score:.2%})"
        
        # Check 2: Has suspicious patterns (Discord giveaway scams)
        elif has_suspicious:
            is_scam = True
            reason = f"Pattern Detection"

        return is_scam, score, reason

    def _check_suspicious_patterns(self, text: str) -> bool:
        """Check for known scam patterns - Discord specific."""
        text_lower = text.lower()

        suspicious_patterns = [
            # ===== DISCORD NITRO SCAMS =====
            r'free.*nitro|nitro.*free|nitro.*giveaway|claim.*nitro',
            r'discord.*nitro|nitro.*discord',
            r'discord\.gift|discord\.com/gift|nitro.*claim',

            # ===== CRYPTO/INVESTMENT SCAMS =====
            r'invest.*(?:and|get|return|back)',
            r'guaranteed.*(?:return|profit|income)',
            r'no risk.*(?:invest|profit|money)',
            r'get.*\$.*back|return.*\$.*hours|earn.*quick',
            r'crypto.*(?:free|airdrop|claim)',
            r'bitcoin|ethereum|btc|eth.*(?:free|claim|airdrop)',

            # ===== GIVEAWAY SCAMS =====
            r'giving\s+away|giveaway',
            r'free.*(?:ps5|xbox|iphone|macbook|steam|air|monitor|laptop|ipad|nitro)',
            r'give.*away|claim.*prize|win.*(?:ps5|xbox|macbook|laptop)',
            r'first.*come.*served|first.*served',
            r'limited.*slots?|only.*(?:person|people)',

            # ===== DM/MESSAGE PATTERNS =====
            r'dm\s+(?:me|if|interested)',
            r'message\s+(?:me|if|interested)',
            r'dm\s+asap|message\s+asap',

            # ===== PHISHING SCAMS =====
            r'verify.*account|confirm.*account|validate.*account',
            r'account.*(?:suspended|banned|compromised|flagged)',
            r'click.*verify|verify.*click|urgent.*verify',
            r'urgent.*account|account.*urgent|immediately.*verify',

            # ===== JOB/PAYMENT SCAMS =====
            r'get\s+paid|earn.*money|quick.*cash|make.*money.*fast',
            r'beta.*(?:tester|test)|testing.*paid|paid.*test',
            r'paid.*(?:dm|message)|(?:dm|message).*paid',

            # ===== URGENCY TACTICS =====
            r'act\s+now|hurry|asap|immediately',
            r'limited.*time|don\'t\s+miss|only.*(?:slots?|spots?|available)',
            r'click.*here|click.*link|click.*now',
            r'link.*below|below.*link',

            # ===== URL SHORTENERS & DOMAINS =====
            r'bit\.ly|tinyurl|t\.co|short\.link|link\.shortener',
            r'(?:discord|steam|nitro|crypto|paypal).*(?:free|claim|gift|verify)\.(?:com|net|xyz)',
        ]

        matched_patterns = []
        for pattern in suspicious_patterns:
            if re.search(pattern, text_lower, re.IGNORECASE):
                matched_patterns.append(pattern[:50])

        if matched_patterns:
            print(f"[PATTERNS MATCHED] {len(matched_patterns)} pattern(s):")
            for p in matched_patterns[:3]:
                print(f"  - {p}")

        return len(matched_patterns) > 0
