import discord
from discord.ext import commands
from datetime import datetime
from typing import Optional
from utils.scam_detector import ScamDetector
from utils.logger import setup_logger
from config import Config

logger = setup_logger(__name__)

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
        
        # Ignore bot messages
        if message.author.bot:
            return
        
        # Ignore commands
        if message.content.startswith(self.bot.command_prefix):
            return
        
        # Check if user has whitelisted role
        if isinstance(message.author, discord.Member):
            user_roles = [role.name for role in message.author.roles]
            if any(role in self.whitelisted_roles for role in user_roles):
                return
        
        try:
            # Detect scam
            is_scam, confidence, reason = self.scam_detector.detect(message.content)
            
            if is_scam:
                await self._handle_scam_message(message, confidence, reason)
                
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
        
        logger.warning(
            f"Scam detected from {member.name}#{member.discriminator} "
            f"({member.id}) with confidence {confidence:.2%}"
        )
        
        # Get join date
        joined_at = "Unknown"
        if isinstance(member, discord.Member) and member.joined_at:
            joined_at = member.joined_at.strftime("%Y-%m-%d %H:%M:%S UTC")
        
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
        
        # Send log to private channel
        await self._send_log(message, member, joined_at, confidence, reason)
    
    async def _send_log(
        self,
        message: discord.Message,
        member: discord.Member,
        joined_at: str,
        confidence: float,
        reason: str
    ):
        """Send log to the private logging channel."""
        
        log_channel = self.bot.get_channel(Config.LOG_CHANNEL_ID)
        
        if not log_channel:
            logger.error(f"Log channel {Config.LOG_CHANNEL_ID} not found")
            return
        
        try:
            embed = discord.Embed(
                title="🚨 Scam Message Deleted",
                color=discord.Color.red(),
                timestamp=datetime.utcnow()
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
            embed.add_field(
                name="Message Content",
                value=message.content[:1024] if message.content else "*No content*",
                inline=False
            )
            
            # Add user avatar
            if member.avatar:
                embed.set_thumbnail(url=member.avatar.url)
            
            # Ping server owner
            owner_mention = message.guild.owner.mention if message.guild.owner else ""
            
            await log_channel.send(
                content=owner_mention,
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
