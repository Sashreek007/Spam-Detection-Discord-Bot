# Spam Detection Discord Bot

A powerful Discord bot that automatically detects and removes scam messages from your server using machine learning. The bot uses a fine-tuned BERT model to identify spam, phishing attempts, and other malicious content with high accuracy.

## Features

**Automatic Spam Detection** - Uses ML model to detect scam messages in real-time  
**Auto Message Deletion** - Instantly removes detected spam from your channels  
**Detailed Logging** - Sends comprehensive reports to a private logging channel  
**Owner Notifications** - Pings server owner when spam is detected  
**Role-Based Whitelisting** - Exempt specific roles (Admins, Moderators) from scanning  
**Manual Checking** - Admin command to manually check if text is spam  
**Statistics Dashboard** - View bot stats and configuration  
**Lightweight Model** - Runs efficiently on CPU or GPU  

## Setup Instructions

### 1. Prerequisites

- Python 3.8+
- A Discord server where you have admin permissions
- A Discord bot token

### 2. Installation
**Clone or download the bot files:**
```bash
cd spam_bot
```

**Install dependencies:**
```bash
pip install -r requirements.txt
```

### 3. Discord Bot Setup

1. Go to [Discord Developer Portal](https://discord.com/developers/applications)
2. Click "New Application" and name it
3. Go to the "Bot" section and click "Add Bot"
4. Under "TOKEN", click "Copy" and save this somewhere safe
5. Enable these **Privileged Gateway Intents**:
   - ✅ MESSAGE CONTENT INTENT (critical!)
   - ✅ SERVER MEMBERS INTENT
6. Go to "OAuth2" → "URL Generator"
7. Select scopes: `bot`
8. Select permissions:
   - Read Messages/View Channels
   - Send Messages
   - Manage Messages (critical for deletion!)
   - Embed Links
9. Copy the generated URL and open it to add bot to your server

### 4. Environment Setup

Create a `.env` file in the root directory:

```env
# Your Discord bot token (from Developer Portal)
DISCORD_TOKEN=your_bot_token_here

# The private channel ID where spam logs are sent
# Create a private channel, right-click → Copy Channel ID
LOG_CHANNEL_ID=123456789012345678

# Machine learning model to use
MODEL_NAME=mrm8488/bert-tiny-finetuned-sms-spam-detection

# Confidence threshold (0.0 to 1.0)
# Messages with confidence > this value are marked as spam
# Higher = more strict, lower = more lenient
SCAM_THRESHOLD=0.85

# Environment mode (development or production)
ENVIRONMENT=development
```

**How to get LOG_CHANNEL_ID:**
1. Create a private channel in your server (e.g., `#spam-logs`)
2. Enable Developer Mode in Discord (Settings → Advanced → Developer Mode)
3. Right-click the channel → "Copy Channel ID"
4. Paste this ID in `.env`

### 5. Run the Bot

```bash
python bot.py
```

You should see:
```
Loading model: mrm8488/bert-tiny-finetuned-sms-spam-detection
Model loaded successfully!
[INFO] All cogs loaded successfully
[INFO] Logged in as YourBotName (...)
[INFO] Moderation cog loaded. Monitoring messages...
```

## Usage

### Automatic Detection

Once running, the bot automatically monitors all messages. When spam is detected:

1. **Message is deleted** from the channel
2. **Log entry is created** in your `LOG_CHANNEL_ID` channel with:
   - User who sent the message
   - User ID and join date
   - Confidence score
   - Detection method
   - Full message content (for review)
   - User's avatar
3. **Server owner is pinged** to review the log

### Admin Commands

#### Check Message
```
!check <message text>
```
Manually test if a message would be detected as spam.

**Example:**
```
!check @everyone FREE NITRO CLICK HERE NOW
```

**Response:**
```
Scam Detection Result
Is Scam?: Yes
Confidence: 95.20%
Reason: ML Detection + Suspicious Patterns
Tested Message: @everyone FREE NITRO CLICK HERE NOW
```

#### View Statistics
```
!stats
```
Shows bot statistics and current configuration.

**Response:**
```
Bot Statistics
Servers: 1
Model: mrm8488/bert-tiny-finetuned-sms-spam-detection
Threshold: 85.00%
```

## Configuration

### Adjusting Sensitivity

Edit `.env` to change `SCAM_THRESHOLD`:

```env
# More sensitive (catches more spam, more false positives)
SCAM_THRESHOLD=0.70

# Default (good balance)
SCAM_THRESHOLD=0.85

# Less sensitive (misses more spam, fewer false positives)
SCAM_THRESHOLD=0.95
```

### Whitelisting Roles

Edit `cogs/moderation.py` and update this line:

```python
self.whitelisted_roles = ['Admin', 'Moderator', 'executive', 'chat revive ping']
```

Add or remove role names to whitelist them from spam scanning.

### Choosing a Different Model

Replace `MODEL_NAME` in `.env` with other spam detection models:

- `mrm8488/bert-tiny-finetuned-sms-spam-detection` (recommended - small, fast)
- `mariagrandury/roberta-base-finetuned-sms-spam-detection` (more accurate, larger)
- `unitary/toxic-bert` (detects toxic/hateful content)

## File Structure

```
spam_bot/
├── bot.py                          # Main bot entry point
├── config.py                       # Configuration loader
├── requirements.txt                # Python dependencies
├── .env                            # Environment variables
├── .env.example                    # Example env file
├── .gitignore                      # Git ignore file
├── LICENSE                         # License file
├── README.md                       # Documentation
├── utils/
│   ├── __init__.py                # Package marker
│   ├── logger.py                  # Logging setup
│   ├── scam_detector.py           # ML model for spam detection
│   └── config.py                  # (optional - if moved to utils)
└── cogs/
    ├── __init__.py                # Package marker
    └── moderation.py              # Moderation commands and message monitoring
```

## Testing

### Test Messages

Try sending these messages to test the bot:

**Strong Spam (should be detected):**
```
@everyone 🎉 FREE DISCORD NITRO GIVEAWAY! 🎉 Click here to claim your free 3 months of Nitro: https://discord-nitro-free.com/claim Limited time only!
```

**Phishing (should be detected):**
```
⚠️ URGENT: Your Discord account has been flagged for suspicious activity. Verify your account now: bit.ly/verify-discord
```

**Crypto Scam (should be detected):**
```
💰 FREE ETHEREUM AIRDROP! 💰 Claim 0.5 ETH instantly! No verification needed: eth-airdrop-free.net
```

**Legitimate Message (should pass):**
```
Hey everyone! Check out this cool Python tutorial: https://realpython.com/python-basics/
```

### Debug Mode

To see detailed logs of what the bot is doing, set:
```env
ENVIRONMENT=development
```

This will show `[DEBUG]` messages in the console for every message processed.

## Troubleshooting

### Bot is running but messages aren't being detected

**Check 1: MESSAGE_CONTENT Intent**
- Go to Discord Developer Portal
- Select your bot
- Go to "Bot" section
- Verify "MESSAGE CONTENT INTENT" is enabled (toggle is blue)
- Restart your bot

**Check 2: Bot Permissions**
- In your server, right-click your bot role
- Ensure it has these permissions:
  - Read Messages/View Channels
  - Send Messages
  - **Manage Messages** (essential!)
  - Embed Links

**Check 3: Bot Role Hierarchy**
- Your bot's role must be **above** the users' roles
- Go to Server Settings → Roles
- Drag your bot's role above other roles

### Messages aren't being deleted

The bot likely doesn't have "Manage Messages" permission. See **Bot Permissions** above.

### No log appears in the private channel

1. Verify `LOG_CHANNEL_ID` in `.env` is correct
2. Check the bot can send messages in that channel
3. Check the channel exists and is private

### Model loading fails

If you see errors like `Model not found`, make sure you have an internet connection - the model downloads on first run (~500MB).

### Bot crashes with import error

Make sure `__init__.py` exists in the `cogs/` folder:
```bash
touch cogs/__init__.py
```

## Advanced Configuration

### Using GPU

Edit `scam_detector.py`:
```python
# Change from:
device=-1  # CPU

# To:
device=0   # GPU (CUDA)
```

Requires GPU and PyTorch GPU support.

### Custom Model Training

To fine-tune the model on your server's data:

1. Collect examples of spam messages your server receives
2. Label them as spam/ham
3. Fine-tune the BERT model using Hugging Face `transformers`
4. Replace `MODEL_NAME` in `.env` with your model

See [Hugging Face Fine-tuning Guide](https://huggingface.co/docs/transformers/training)

## Performance

- **Model Size:** ~18MB (tiny BERT)
- **Inference Time:** 0.1-0.3 seconds per message
- **Accuracy:** ~92-96% on spam detection tasks
- **Memory:** ~100-200MB RAM
- **CPU:** Can run on standard CPU
- **GPU:** Optional (much faster)

## Privacy & Security

- Messages are only analyzed locally (not sent to external APIs)
- Logs are stored only in your private Discord channel
- Bot token is stored in `.env` (never commit this to version control)
- Only admins can use manual check commands

## Support & Issues

If you encounter issues:

1. **Check the console logs** for error messages
2. **Enable debug mode** in `.env` to see detailed logs
3. **Verify all permissions** are granted to the bot
4. **Restart the bot** after making changes

## Requirements

See `requirements.txt`:
```
discord.py>=2.3.2
python-dotenv>=1.0.0
transformers>=4.35.0
torch>=2.0.0
aiohttp>=3.9.0
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Next Steps

1. Set up Discord bot token and permissions
2. Create `.env` file with configuration
3. Install dependencies: `pip install -r requirements.txt`
4. Run the bot: `python bot.py`
5. Test with spam messages
6. Adjust `SCAM_THRESHOLD` based on results
7. Whitelist trusted roles if needed

Good luck protecting your server! 
