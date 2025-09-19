#!/usr/bin/env python3
"""
Скрипт для запуска BI-GPT Agent с выбором модели
"""

import os
import sys
import subprocess
import argparse

def main():
    parser = argparse.ArgumentParser(description='BI-GPT Agent Launcher')
    parser.add_argument('--model', choices=['finetuned', 'custom_api', 'integrated'], 
                       default='integrated', help='Выберите модель для запуска')
    parser.add_argument('--port', type=int, default=8501, help='Порт для Streamlit')
    
    args = parser.parse_args()
    
    print("🚀 Запуск BI-GPT Agent")
    print(f"📱 Модель: {args.model}")
    print(f"🌐 Порт: {args.port}")
    print("-" * 50)
    
    if args.model == 'integrated':
        print("🔧 Запускаем интегрированное приложение с выбором модели...")
        cmd = f"streamlit run integrated_app.py --server.port {args.port}"
    elif args.model == 'finetuned':
        print("🧠 Запускаем приложение с Fine-tuned Phi-3 + LoRA...")
        cmd = f"streamlit run streamlit_app.py --server.port {args.port}"
    elif args.model == 'custom_api':
        print("🌐 Запускаем приложение с Custom API моделью...")
        cmd = f"streamlit run streamlit_app.py --server.port {args.port}"
    
    print(f"💻 Команда: {cmd}")
    print("🌐 Откройте браузер по адресу: http://localhost:8501")
    print("⏹️  Для остановки нажмите Ctrl+C")
    print("-" * 50)
    
    try:
        subprocess.run(cmd, shell=True)
    except KeyboardInterrupt:
        print("\n👋 Приложение остановлено")

if __name__ == "__main__":
    main()
