#![allow(deprecated)]

use serenity::client::Context;
use std::env;
use std::fs::File;
use std::path::Path;
use std::time::Duration;
use symphonia::default::formats::OggReader;
use symphonia_core::formats::FormatReader;

use bytes::Bytes;
use ringbuf::traits::{Producer, Split};
use ringbuf::HeapRb;
use serenity::async_trait;
use serenity::framework::standard::macros::{command, group};
use serenity::framework::standard::{Args, CommandResult, StandardFramework};
use serenity::model::channel::Message;
use serenity::model::gateway::Ready;
use serenity::prelude::*;
use songbird::SerenityInit;
use symphonia::core::io::MediaSourceStream;

struct Handler;

#[async_trait]
impl EventHandler for Handler {
    async fn ready(&self, _: Context, ready: Ready) {
        println!("{} is connected!", ready.user.name);
    }
}

#[group]
#[commands(join, leave, play_direct)]
struct General;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let token = env::var("DISCORD_TOKEN").expect("Expected a token in the environment");

    let framework = StandardFramework::new().group(&GENERAL_GROUP);

    let intents = GatewayIntents::non_privileged()
        | GatewayIntents::MESSAGE_CONTENT
        | GatewayIntents::GUILD_VOICE_STATES;

    let mut client = Client::builder(&token, intents)
        .event_handler(Handler)
        .framework(framework)
        .register_songbird()
        .await
        .expect("Err creating client");

    let _ = client
        .start()
        .await
        .map_err(|why| println!("Client ended: {:?}", why));
}

#[command]
#[only_in(guilds)]
async fn join(ctx: &Context, msg: &Message) -> CommandResult {
    let (guild_id, channel_id) = {
        let guild_id = msg.guild_id.unwrap();
        let channel_id = guild_id
            .to_guild_cached(&ctx.cache)
            .map(|guild| {
                guild
                    .voice_states
                    .get(&msg.author.id)
                    .and_then(|voice_state| voice_state.channel_id)
            })
            .unwrap_or(None);

        (guild_id, channel_id)
    };

    let connect_to = match channel_id {
        Some(channel) => channel,
        None => {
            msg.reply(ctx, "Not in a voice channel").await?;
            return Ok(());
        },
    };

    let manager = songbird::get(ctx)
        .await
        .expect("Songbird Voice client placed in at initialization.")
        .clone();

    if let Ok(_handler_lock) = manager.join(guild_id, connect_to).await {
        msg.channel_id.say(&ctx.http, "Joined!").await?;
    }

    Ok(())
}

#[command]
#[only_in(guilds)]
async fn leave(ctx: &Context, msg: &Message) -> CommandResult {
    let guild_id = msg.guild_id.unwrap();

    let manager = songbird::get(ctx)
        .await
        .expect("Songbird Voice client placed in at initialization.")
        .clone();

    if manager.get(guild_id).is_some() {
        manager.remove(guild_id).await?;
        msg.channel_id.say(&ctx.http, "Left!").await?;
    } else {
        msg.reply(ctx, "Not in a voice channel").await?;
    }

    Ok(())
}

#[command]
#[only_in(guilds)]
async fn play_direct(ctx: &Context, msg: &Message, mut args: Args) -> CommandResult {
    let url = match args.single::<String>() {
        Ok(url) => url,
        Err(_) => {
            msg.channel_id
                .say(&ctx.http, "Must provide a path to an .ogg/opus file")
                .await?;
            return Ok(());
        },
    };

    if !Path::new(&url).exists() {
        msg.channel_id.say(&ctx.http, "File not found").await?;
        return Ok(());
    }

    let guild_id = msg.guild_id.unwrap();

    let manager = songbird::get(ctx)
        .await
        .expect("Songbird Voice client placed in at initialization.")
        .clone();

    if let Some(handler_lock) = manager.get(guild_id) {
        let handler = handler_lock.lock().await;

        // 1. Create the ringbuffer
        let (mut prod, cons) = HeapRb::<Bytes>::new(100).split();

        // 2. Hand the consumer to Songbird
        handler.play_direct_opus(cons);

        // 3. Spawn the feeder thread
        tokio::spawn(async move {
            if let Err(e) = feeder_task(url, &mut prod).await {
                eprintln!("Feeder task error: {:?}", e);
            }
        });

        msg.channel_id
            .say(&ctx.http, "Starting direct Opus playback!")
            .await?;
    } else {
        msg.reply(ctx, "Not in a voice channel to play in").await?;
    }

    Ok(())
}

async fn feeder_task(
    path: String,
    prod: &mut ringbuf::HeapProd<Bytes>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let file = File::open(path)?;
    let mss = MediaSourceStream::new(Box::new(file), Default::default());

    let mut reader = OggReader::try_new(mss, &Default::default())?;

    // Find the first Opus track
    /*
    let track = reader.default_track().ok_or("No Opus track found")?;
    let track_id = track.id;
    println!("Starting playback of Opus track {}", track_id);
    */

    loop {
        let packet = match reader.next_packet() {
            Ok(packet) => packet,
            Err(symphonia::core::errors::Error::IoError(ref e))
                if e.kind() == std::io::ErrorKind::UnexpectedEof =>
            {
                break;
            },
            Err(e) => return Err(e.into()),
        };

        /*
        if packet.track_id() != track_id {
            continue;
        }
        */

        let data = Bytes::copy_from_slice(&packet.data);

        // Push to ringbuffer, sleep if full
        while let Err(_returned_data) = prod.try_push(data.clone()) {
            tokio::time::sleep(Duration::from_millis(5)).await;
            // In a real app, you might want a way to cancel this loop if the driver stops
        }
    }

    println!("Finished feeding Opus frames");
    Ok(())
}
