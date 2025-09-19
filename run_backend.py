#!/usr/bin/env python3
"""
Скрипт для запуска backend приложения из корневой директории
"""
import subprocess
import sys
import os

def main():
    """Запуск Streamlit приложения"""
    backend_dir = os.path.join(os.path.dirname(__file__), 'backend')
    app_path = os.path.join(backend_dir, 'app', 'presentation', 'streamlit_app.py')
    
    if not os.path.exists(app_path):
        print(f"❌ Файл приложения не найден: {app_path}")
        return False
    
    print("🚀 Запуск Streamlit приложения...")
    print(f"📁 Путь к приложению: {app_path}")
    print("🌐 Приложение будет доступно по адресу: http://localhost:8501")
    print("⏹️  Для остановки нажмите Ctrl+C")
    print("-" * 50)
    
    try:
        # Переходим в папку backend и запускаем Streamlit
        subprocess.run([
            sys.executable, "-m", "streamlit", "run", 
            "app/presentation/streamlit_app.py", 
            "--server.port", "8501", "--server.address", "localhost"
        ], cwd=backend_dir, check=True)
    except KeyboardInterrupt:
        print("\n⏹️  Приложение остановлено пользователем")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Ошибка запуска приложения: {e}")
        return False
    except Exception as e:
        print(f"❌ Неожиданная ошибка: {e}")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
