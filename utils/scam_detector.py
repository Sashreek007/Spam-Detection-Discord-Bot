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

        # Run ML model
        result = self.classifier(text)[0]
        score = result['score']
        label = result['label']

        # Map numeric labels to text
        label_map = {
            'LABEL_0': 'HAM',   # Legitimate message
            'LABEL_1': 'SPAM'   # Spam/Scam message
        }
        label = label_map.get(label, label).upper()

        # Check for suspicious patterns
        has_suspicious = self._check_suspicious_patterns(text)

        # DEBUG OUTPUT
        print(f"\n[DETECTOR] Label: {label} | Score: {score:.4f} | Suspicious: {has_suspicious}")
        print(f"[DETECTOR] Threshold: {Config.SCAM_THRESHOLD}")
        print(f"[DETECTOR] Check (label=SPAM AND score > threshold AND suspicious): {label == 'SPAM'} and {score > Config.SCAM_THRESHOLD} and {has_suspicious}\n")

        # Determine if it's a scam
        is_scam = False
        reason = ""

        # Only flag as scam if ALL conditions are met:
        # 1. Model says SPAM (label=SPAM)
        # 2. High confidence (score > threshold)
        # 3. AND has suspicious patterns (to reduce false positives)
        if label == 'SPAM' and score > Config.SCAM_THRESHOLD and has_suspicious:
            is_scam = True
            reason = f"ML Detection + Suspicious Patterns ({score:.2%})"

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
            r'(?:free|instant).*crypto',

            # ===== GIVEAWAY SCAMS =====
            r'giving\s+away|giveaway',
            r'give.*away|claim.*prize|win.*(?:ps5|xbox|macbook|laptop)',
            r'limited.*slots?|first.*(?:people|members|users)',
            r'free.*(?:ps5|xbox|iphone|macbook|steam|air|monitor|laptop|ipad)',
            r'free.*come.*served|first.*come.*free',

            # ===== GIVEAWAY + DM PATTERN =====
            r'@everyone.*free|free.*@everyone',
            r'@here.*free|free.*@here',
            r'free.*(?:dm|message|dm\s+me)',
            r'giveaway.*dm|dm.*giveaway',
            r'(?:dm|message).*(?:interested|if|you)',
            r'dm\s+(?:if|me|interested)',
            r'message\s+(?:if|me|interested)',

            # ===== PHISHING SCAMS =====
            r'verify.*account|confirm.*account|validate.*account',
            r'account.*(?:suspended|banned|compromised|flagged)',
            r'click.*verify|verify.*click|urgent.*verify',
            r'urgent.*account|account.*urgent|immediately.*verify',

            # ===== JOB/PAYMENT SCAMS =====
            r'get\s+paid|earn.*money|quick.*cash|make.*money.*fast',
            r'beta.*(?:tester|test)|testing.*paid|paid.*test',
            r'\$.*(?:guarantee|guaranteed)',
            r'paid.*(?:dm|message)|(?:dm|message).*paid',

            # ===== COMMON SCAM TACTICS =====
            r'act now|hurry|limited time|don\'t miss|only.*(?:slots?|spots?|available)',
            r'click.*here|click.*link|click.*now',
            r'link.*below|below.*link',
            r'dm.*(?:for|details|info)',
            r'bit\.ly|tinyurl|t\.co|short\.link|link\.shortener',

            # ===== MALICIOUS DOMAINS =====
            r'(?:discord|steam|nitro|crypto|paypal).*(?:free|claim|gift|verify)\.(?:com|net|xyz|click|site)',
            r'(?:free|claim|win).*(?:\.com|\.net|\.xyz)',

            # ===== URGENCY + MONEY COMBO =====
            r'(?:act|click|verify|confirm|hurry).*(?:now|fast|urgent|asap)',
            r'(?:limited|only|first|last).*\d+',
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
