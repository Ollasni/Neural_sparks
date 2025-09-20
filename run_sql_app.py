#!/usr/bin/env python3
"""
Скрипт для запуска Streamlit приложения с функциональностью SQL запросов
"""

import os
import sys
import subprocess
from pathlib import Path

def main():
    """Запускает Streamlit приложение"""
    
    # Получаем путь к директории проекта
    project_root = Path(__file__).parent
    backend_path = project_root / "backend"
    streamlit_app_path = backend_path / "app" / "presentation" / "streamlit_app.py"
    
    # Проверяем существование файла
    if not streamlit_app_path.exists():
        print(f"❌ Файл приложения не найден: {streamlit_app_path}")
        sys.exit(1)
    
    # Переходим в директорию backend
    os.chdir(backend_path)
    
    print("🚀 Запуск Streamlit приложения с SQL функциональностью...")
    print(f"📁 Рабочая директория: {backend_path}")
    print(f"📄 Файл приложения: {streamlit_app_path}")
    print()
    print("🌐 Приложение будет доступно по адресу: http://localhost:8501")
    print("💡 Используйте Ctrl+C для остановки")
    print()
    
    try:
        # Запускаем Streamlit
        subprocess.run([
            sys.executable, "-m", "streamlit", "run", 
            str(streamlit_app_path),
            "--server.port", "8501",
            "--server.address", "0.0.0.0",
            "--browser.gatherUsageStats", "false"
        ], check=True)
    except KeyboardInterrupt:
        print("\n👋 Приложение остановлено пользователем")
    except subprocess.CalledProcessError as e:
        print(f"❌ Ошибка запуска приложения: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
