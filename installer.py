import asyncio
import subprocess

from datetime import datetime


input_stream = "rtmp://127.0.0.1/live/stream"
encoder = "libx264"






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