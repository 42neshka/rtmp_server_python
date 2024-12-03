import asyncio
import subprocess

from datetime import datetime


input_stream = "rtmp://127.0.0.1/live/stream"
encoder = "libx264"



# Список платформ для стрима
platforms = [
    {"url": "rtmp://live.twitch.tv/app/live_1128390212_GAt9MBDQ2RUg2sQVwhuwm0AdtOOw5V",
     "key": "live_1128390212_GAt9MBDQ2RUg2sQVwhuwm0AdtOOw5V",
     "bitrate": "5000k",
     "encoder": encoder},
    {"url": "rtmp://livepush.trovo.live/live/",
     "key": "73846_116420352_116420352?bizid=73846&txSecret=cdd812c92cc71b7aafe2dd9dd13c3a1b&txTime=79885983&cert=52376e54effbd2f1a0bad52823644940&certTime=66bc5683&flag=txcloud_116420352_116420352&timeshift_bps=0%7C2500%7C1500&timeshift_dur=43200&txAddTimestamp=4&tp_code=1723618947&tp_sign=355285362&dm_sign=760340275&pq_sn=804972357&txHost=livepush.trovo.live",
     "bitrate": "2000k",
     "encoder": encoder},
]


# Функция запуска стрима через FFmpeg
async def start_stream(index, input_url, output_url, bitrate, encoder):
    command = [
        "ffmpeg",
        "-i", input_url,               # Входной поток (RTMP от OBS)
        "-c:v", encoder,               # Кодек для видео
        "-b:v", bitrate,               # Битрейт видео
        "-vf", "scale=1920:1080",      # Масштабирование до 1920x1080
        "-bf", "2",                    # Количество B-frames
        "-g", "30",                    # Частота ключевых кадров (GOP)
        "-c:a", "aac",                 # Кодек для аудио
        "-b:a", "128k",                # Битрейт аудио
        "-f", "flv", output_url        # Формат и выходной поток
    ]

    # Запуск FFmpeg процесса
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Запуск потока {index + 1} → Ready")
    process = await asyncio.create_subprocess_exec(
        *command,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL
    )
    await process.wait()
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Поток {index + 1} завершён.")


# Основная функция для запуска потоков
async def main():
    # Запуск потоков
    tasks = [
        start_stream(i, input_stream, f"{platform['url']}/{platform['key']}", platform["bitrate"], platform["encoder"])
        for i, platform in enumerate(platforms)
    ]

    # Ожидание завершения всех потоков
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[!] Скрипт остановлен.")