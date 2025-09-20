from dataclasses import dataclass
from typing import Optional, Dict, Any
import os
import sys

# Добавляем путь к корневой директории проекта
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', '..'))

from config import get_settings

@dataclass
class UIText:
    title = "Подключитесь к своей базе данных и задавайте вопросы на естественном языке"
    schema_title = "🧱 Схема базы данных"
    connect_success = "Подключение успешно!"
    connect_fail = "Ошибка подключения"

def get_database_config() -> Dict[str, Any]:
    """Возвращает конфигурацию базы данных"""
    settings = get_settings()
    return settings.get_database_config()

def get_database_url() -> str:
    """Возвращает URL базы данных"""
    settings = get_settings()
    return settings.get_database_url()
