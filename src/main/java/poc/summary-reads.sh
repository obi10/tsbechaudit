#!/bin/bash

# Ruta base de logs
LOG_DIR="/home/opc/Documents/cursor_projects/tsbechaudit/log"

# Archivos consolidados
READ_CSV="$LOG_DIR/read_thread.csv"
WRITE_CSV="$LOG_DIR/write_thread.csv"

# Limpiar los CSV consolidados anteriores
rm -f "$READ_CSV" "$WRITE_CSV"

# Procesar logs de lectura
for file in "$LOG_DIR"/read*.log; do
  if [ -f "$file" ]; then
    # Eliminar última línea si está vacía o incompleta (menos de 2 campos separados por coma)
    total_lines=$(wc -l < "$file")
    last_line=$(tail -n 1 "$file")
    field_count=$(echo "$last_line" | awk -F',' '{print NF}')
    
    if [ "$field_count" -lt 2 ]; then
      # Omitir la última línea al concatenar
      head -n $((total_lines - 1)) "$file" >> "$READ_CSV"
    else
      cat "$file" >> "$READ_CSV"
    fi
  fi
done

# Procesar logs de escritura
for file in "$LOG_DIR"/write*.log; do
  if [ -f "$file" ]; then
    total_lines=$(wc -l < "$file")
    last_line=$(tail -n 1 "$file")
    field_count=$(echo "$last_line" | awk -F',' '{print NF}')
    
    if [ "$field_count" -lt 2 ]; then
      head -n $((total_lines - 1)) "$file" >> "$WRITE_CSV"
    else
      cat "$file" >> "$WRITE_CSV"
    fi
  fi
done

# Ejecutar script de resumen
python3 ./summary-reads.py