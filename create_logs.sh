#!/bin/bash

# Скрипт создания лог-файлов для cron задач
# Выполнять с правами root: sudo ./create_logs.sh

LOG_DIR="/var/log"
LOG_FILES=(
  "ym_hits_dwnl_daily.log"
  "ym_visits_dwnl_daily.log"
  "ym_hits_params_dwnl_daily.log"
  "cdm_daily_update.log"
)

# Создаем директорию если не существует
mkdir -p "$LOG_DIR"

# Создаем каждый лог-файл
for file in "${LOG_FILES[@]}"; do
  full_path="${LOG_DIR}/${file}"
  
  # Создаем файл если не существует
  if [ ! -f "$full_path" ]; then
    touch "$full_path"
    echo "Создан файл: $full_path"
  else
    echo "Файл уже существует: $full_path"
  fi

  # Устанавливаем владельца и права
  chown root:root "$full_path"
  chmod 644 "$full_path"  # -rw-r--r--
  echo "Установлены права для: $full_path"
done

echo "Все лог-файлы созданы и настроены"
