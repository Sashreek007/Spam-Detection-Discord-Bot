import discord
from discord.ext import commands
import asyncio
from config import Config
from utils.logger import setup_logger

# Validate configuration
Config.validate()

# Setup logger
logger = setup_logger(__name__)

# Bot setup
intents = discord.Intents.default()
intents.message_content = True
intents.members = True

bot = commands.Bot(
    command_prefix='!',
    intents=intents,
    help_command=commands.DefaultHelpCommand()
)

@bot.event
async def on_ready():
    logger.info(f'Logged in as {bot.user.name} ({bot.user.id})')
    logger.info(f'Connected to {len(bot.guilds)} servers')
    logger.info('Bot is ready!')
    
    # Set bot status
    await bot.change_presence(
        activity=discord.Activity(
            type=discord.ActivityType.watching,
            name="for scam messages"
        )
    )

@bot.event
async def on_command_error(ctx: commands.Context, error: commands.CommandError):
    """Handle command errors."""
    
    if isinstance(error, commands.CommandNotFound):
        return
    elif isinstance(error, commands.MissingPermissions):
        await ctx.send("❌ You don't have permission to use this command.")
    elif isinstance(error, commands.MissingRequiredArgument):
        await ctx.send(f"❌ Missing required argument: {error.param.name}")
    else:
        logger.error(f"Command error: {error}", exc_info=True)
        await ctx.send("❌ An error occurred while processing the command.")

async def load_extensions():
    """Load all cogs."""
    await bot.load_extension('cogs.moderation')
    logger.info("All cogs loaded successfully")

async def main():
    """Main bot entry point."""
    async with bot:
        await load_extensions()
        await bot.start(Config.DISCORD_TOKEN)

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot shutdown requested")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
