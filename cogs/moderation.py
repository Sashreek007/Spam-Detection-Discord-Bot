import discord
from discord.ext import commands
from datetime import datetime
from typing import Optional
import sys
from pathlib import Path
import pytz

from utils.scam_detector import ScamDetector
from utils.logger import setup_logger
from config import Config

# Initialize logger
logger = setup_logger(__name__)

# Set timezone to Edmonton
LOCAL_TZ = pytz.timezone('America/Edmonton')

class ModerationCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.scam_detector = ScamDetector()
        self.whitelisted_roles = ['Admin', 'Moderator', 'executive', 'chat revive ping']  # Add role names to whitelist
        
    @commands.Cog.listener()
    async def on_ready(self):
        logger.info(f'Moderation cog loaded. Monitoring messages...')
    
    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        """Monitor all messages for scam content."""
        
        logger.info(f"[DEBUG] Received message from {message.author.name}: {message.content[:80]}")
        
        # Ignore bot messages
        if message.author.bot:
            logger.info("[DEBUG] Ignoring bot message")
            return
        
        # Ignore commands
        if message.content.startswith(self.bot.command_prefix):
            logger.info("[DEBUG] Ignoring command")
            return
        
        # Check if user has whitelisted role
        if isinstance(message.author, discord.Member):
            user_roles = [role.name for role in message.author.roles]
            logger.info(f"[DEBUG] User roles: {user_roles}")
            if any(role in self.whitelisted_roles for role in user_roles):
                logger.info("[DEBUG] User has whitelisted role, skipping")
                return
        
        try:
            logger.info(f"[DEBUG] Analyzing message: {message.content[:100]}")
            # Detect scam
            is_scam, confidence, reason = self.scam_detector.detect(message.content)
            logger.info(f"[DEBUG] Detection result: is_scam={is_scam}, confidence={confidence:.2%}, reason={reason}")
            
            if is_scam:
                logger.warning(f"[SCAM DETECTED] Processing message from {message.author.name}")
                await self._handle_scam_message(message, confidence, reason)
            else:
                logger.info("[DEBUG] Message is clean")
                
        except Exception as e:
            logger.error(f"Error processing message: {e}", exc_info=True)
    
    async def _handle_scam_message(
        self, 
        message: discord.Message, 
        confidence: float, 
        reason: str
    ):
        """Handle detected scam message."""
        
        member = message.author
        guild = message.guild
        
        # Store original message time in Edmonton timezone
        message_sent_time = message.created_at.astimezone(LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S %Z")
        
        logger.warning(
            f"Scam detected from {member.name}#{member.discriminator} "
            f"({member.id}) with confidence {confidence:.2%} at {message_sent_time}"
        )
        
        # Get join date
        joined_at = "Unknown"
        if isinstance(member, discord.Member) and member.joined_at:
            joined_at = member.joined_at.astimezone(LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S %Z")
        
        # Delete the message
        try:
            await message.delete()
            logger.info(f"Deleted scam message from {member.name}")
        except discord.errors.Forbidden:
            logger.error("Bot lacks permission to delete messages")
            return
        except Exception as e:
            logger.error(f"Error deleting message: {e}")
            return
        
        # Send DM notification to the user
        await self._send_user_notification(member, guild)
        
        # Send log to private channel
        await self._send_log(message, member, joined_at, confidence, reason, message_sent_time)
    
    async def _send_user_notification(self, member: discord.Member, guild: discord.Guild):
        """Send a DM notification to the user whose message was flagged."""
        
        try:
            embed = discord.Embed(
                title="⚠️ Message Flagged",
                description=(
                    f"Your recent message in **{guild.name}** has been flagged by our automated "
                    "moderation system and removed."
                ),
                color=discord.Color.orange()
            )
            
            embed.add_field(
                name="What does this mean?",
                value=(
                    "Our system detected content that may violate server rules. "
                    "If you believe this was a mistake, please don't worry!"
                ),
                inline=False
            )
            
            embed.add_field(
                name="Was this a false alarm?",
                value=(
                    "Please feel free to contact the server administrators or moderators. "
                    "Your message has been logged, and if this was indeed an error, "
                    "we can review it and restore it if needed."
                ),
                inline=False
            )
            
            embed.add_field(
                name="Need help?",
                value=(
                    "Reach out to the server admins through the server's modmail or "
                    "contact channels. They'll be happy to assist you!"
                ),
                inline=False
            )
            
            embed.set_footer(text="Automated Security System")
            
            await member.send(embed=embed)
            logger.info(f"Successfully sent DM notification to {member.name}")
            
        except discord.errors.Forbidden:
            # User has DMs disabled or has blocked the bot
            logger.warning(
                f"Could not send DM to {member.name} (DMs disabled or bot blocked)"
            )
        except Exception as e:
            logger.error(f"Error sending DM notification to {member.name}: {e}", exc_info=True)
    
    async def _send_log(
        self,
        message: discord.Message,
        member: discord.Member,
        joined_at: str,
        confidence: float,
        reason: str,
        message_sent_time: str
    ):
        """Send log to the private logging channel."""
        
        log_channel = self.bot.get_channel(Config.LOG_CHANNEL_ID)
        
        if not log_channel:
            logger.error(f"Log channel {Config.LOG_CHANNEL_ID} not found")
            return
        
        try:
            # Get current detection time in Edmonton timezone
            detected_at = datetime.now(LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S %Z")
            
            embed = discord.Embed(
                title="🚨 Scam Message Deleted",
                color=discord.Color.red(),
                timestamp=datetime.now(LOCAL_TZ)
            )
            
            embed.add_field(
                name="User",
                value=f"{member.mention} ({member.name}#{member.discriminator})",
                inline=False
            )
            embed.add_field(name="User ID", value=str(member.id), inline=True)
            embed.add_field(name="Joined Server", value=joined_at, inline=True)
            embed.add_field(name="Detection Method", value=reason, inline=False)
            embed.add_field(name="Confidence", value=f"{confidence:.2%}", inline=True)
            embed.add_field(name="Channel", value=message.channel.mention, inline=True)
            
            # Add timestamps
            embed.add_field(name="Message Sent", value=message_sent_time, inline=True)
            embed.add_field(name="Detected At", value=detected_at, inline=True)
            
            embed.add_field(
                name="Message Content",
                value=message.content[:1024] if message.content else "*No content*",
                inline=False
            )
            
            # Add user avatar
            if member.avatar:
                embed.set_thumbnail(url=member.avatar.url)
            
            # Add footer indicating if DM was sent
            embed.set_footer(text="User has been notified via DM")
            
            # Ping moderator role
            mod_role = message.guild.get_role(Config.MODERATOR_ROLE_ID)
            
            if mod_role:
                role_mention = mod_role.mention
                content = f"{role_mention} Spam detected!"
            else:
                logger.warning(f"Moderator role {Config.MODERATOR_ROLE_ID} not found")
                content = "Spam detected!"
            
            await log_channel.send(
                content=content,
                embed=embed
            )
            
            logger.info(f"Sent log to channel {log_channel.name}")
            
        except Exception as e:
            logger.error(f"Error sending log: {e}", exc_info=True)
    
    @commands.command(name='check')
    @commands.has_permissions(administrator=True)
    async def check_message(self, ctx: commands.Context, *, text: str):
        """Manually check if a message is a scam (Admin only)."""
        
        is_scam, confidence, reason = self.scam_detector.detect(text)
        
        embed = discord.Embed(
            title="Scam Detection Result",
            color=discord.Color.red() if is_scam else discord.Color.green()
        )
        
        embed.add_field(name="Is Scam?", value="Yes" if is_scam else "No", inline=True)
        embed.add_field(name="Confidence", value=f"{confidence:.2%}", inline=True)
        embed.add_field(name="Reason", value=reason or "N/A", inline=False)
        embed.add_field(name="Tested Message", value=text[:1024], inline=False)
        
        await ctx.send(embed=embed)
    
    @commands.command(name='stats')
    @commands.has_permissions(administrator=True)
    async def show_stats(self, ctx: commands.Context):
        """Show bot statistics (Admin only)."""
        
        embed = discord.Embed(
            title="Bot Statistics",
            color=discord.Color.blue()
        )
        
        embed.add_field(name="Servers", value=len(self.bot.guilds), inline=True)
        embed.add_field(name="Model", value=Config.MODEL_NAME, inline=False)
        embed.add_field(name="Threshold", value=f"{Config.SCAM_THRESHOLD:.2%}", inline=True)
        
        await ctx.send(embed=embed)

async def setup(bot: commands.Bot):
    await bot.add_cog(ModerationCog(bot))