

import os
import sys
import time
import subprocess
import argparse
from pathlib import Path

def print_logo():
    """Логотип системы"""
    print("""
BI-GPT Agent v1.0
Natural Language to SQL for corporate BI
Powered by Llama-4-Scout
""")

def check_system_requirements():
    """Проверка системных требований"""
    print("Checking system requirements...")
    
    # Проверка Python версии
    if sys.version_info < (3, 8):
        print("ERROR: Python 3.8+ required")
        return False
    print(f"OK: Python {sys.version.split()[0]}")
    
    # Проверка зависимостей
    required_packages = [
        'openai', 'streamlit', 'pandas', 'plotly', 
        'sqlalchemy', 'pydantic'
    ]
    
    missing_packages = []
    for package in required_packages:
        try:
            __import__(package)
            print(f"OK: {package}")
        except ImportError:
            missing_packages.append(package)
            print(f"MISSING: {package}")
    
    if missing_packages:
        print(f"\nMissing packages: {', '.join(missing_packages)}")
        print("Install dependencies: pip install -r requirements.txt")
        return False
    
    return True

def test_local_model_connection():
    """Быстрый тест подключения к модели"""
    print("\nTesting connection to Llama-4-Scout...")
    
    try:
        from bi_gpt_agent import BIGPTAgent
        
        # Настройки локальной модели
        api_key = ""
        base_url = "https://bkwg3037dnb7aq-8000.proxy.runpod.net/v1"
        
        print(f"URL: {base_url}")
        print(f"API Key: {api_key[:10]}...")
        
        # Инициализация агента
        agent = BIGPTAgent(api_key=api_key, base_url=base_url)
        
        # Простой тест
        result = agent.process_query("покажи всех клиентов")
        
        if 'error' not in result:
            print("OK: Model responds correctly")
            print(f"Generated SQL: {result['sql'][:50]}...")
            return True
        else:
            print(f"ERROR: Model error: {result['error']}")
            return False
            
    except Exception as e:
        print(f"ERROR: Connection failed: {e}")
        return False

def run_quick_demo():
    """Быстрая демонстрация возможностей"""
    print("\nRunning quick demo...")
    
    try:
        from bi_gpt_agent import BIGPTAgent
        
        # Инициализация с локальной моделью
        agent = BIGPTAgent(
            api_key="",
            base_url="https://bkwg3037dnb7aq-8000.proxy.runpod.net/v1"
        )
        
        # Демо запросы
        demo_queries = [
            "покажи всех клиентов",
            "количество заказов",
            "средний чек клиентов"
        ]
        
        print("Testing queries:")
        successful = 0
        
        for i, query in enumerate(demo_queries, 1):
            print(f"\n{i}. '{query}'")
            
            start_time = time.time()
            result = agent.process_query(query)
            exec_time = time.time() - start_time
            
            if 'error' not in result:
                successful += 1
                print(f"   SUCCESS: {exec_time:.1f}s")
                print(f"   SQL: {result['sql'][:60]}...")
                print(f"   Rows: {len(result['results'])}")
            else:
                print(f"   ERROR: {result['error']}")
        
        success_rate = (successful / len(demo_queries)) * 100
        print(f"\nSuccess rate: {success_rate:.0f}% ({successful}/{len(demo_queries)})")
        
        if success_rate >= 60:
            print("System working well")
            return True
        else:
            print("System needs improvement")
            return False
            
    except Exception as e:
        print(f"Demo error: {e}")
        return False

def launch_streamlit():
    """Запуск Streamlit интерфейса"""
    print("\nLaunching web interface...")
    print("Open browser at: http://localhost:8501")
    print("System configured for Llama-4-Scout model")
    
    try:
        # Запуск Streamlit
        subprocess.run([
            sys.executable, "-m", "streamlit", "run", 
            "streamlit_app.py",
            "--server.port=8501",
            "--server.address=0.0.0.0"
        ])
    except KeyboardInterrupt:
        print("\nWeb interface stopped")
    except Exception as e:
        print(f"Streamlit error: {e}")

def show_system_info():
    """Показать информацию о системе"""
    print("\nSYSTEM INFO")
    print("=" * 30)
    print("Model: Llama-4-Scout")
    print("Architecture: Multi-Agent System")
    print("Features:")
    print("- Natural Language to SQL")
    print("- Business terminology support")
    print("- Security validation")
    print("- Web interface")

def main():
    """Главная функция запуска"""
    # Парсинг аргументов
    parser = argparse.ArgumentParser(description='BI-GPT Agent Launcher')
    parser.add_argument('--api_key', type=str, 
                       default="",
                       help='API key for the model')
    parser.add_argument('--base_url', type=str,
                       default="https://bkwg3037dnb7aq-8000.proxy.runpod.net/v1",
                       help='Base URL for the model API')
    parser.add_argument('--skip_demo', action='store_true',
                       help='Skip initial demo')
    
    args = parser.parse_args()
    
    print_logo()
    print(f"Model: {args.base_url}")
    print(f"API Key: {args.api_key[:10]}...")
    
    # Проверка требований
    if not check_system_requirements():
        print("\nERROR: System requirements not met")
        return
    
    # Тест модели (если не пропущен)
    if not args.skip_demo:
        if not test_local_model_connection():
            print("\nWARNING: Model connection issues")
            print("System may work with limitations")
        
        # Быстрое демо
        demo_success = run_quick_demo()
        
        # Информация о системе
        show_system_info()
    
    print("\n" + "="*40)
    print("SYSTEM READY")
    print("="*40)
    
    # Опции запуска
    print("\nSelect mode:")
    print("1. Launch web interface")
    print("2. Run console demo")
    print("3. Run tests")
    print("4. Exit")
    
    while True:
        try:
            choice = input("\nYour choice (1-4): ").strip()
            
            if choice == "1":
                launch_streamlit()
                break
            elif choice == "2":
                print("Demo already completed above")
                break
            elif choice == "3":
                subprocess.run([sys.executable, "test_simple.py"])
                break
            elif choice == "4":
                print("Goodbye!")
                break
            else:
                print("Invalid choice, try again")
                
        except KeyboardInterrupt:
            print("\nGoodbye!")
            break
        except Exception as e:
            print(f"Error: {e}")

if __name__ == "__main__":
    main()
