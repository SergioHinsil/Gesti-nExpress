import sqlite3
from datetime import datetime
import openpyxl
from io import StringIO, BytesIO
import csv
import json
from reportlab.lib.pagesizes import A4, landscape
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
from reportlab.lib import colors
from reportlab.lib.units import inch
from reportlab.lib.styles import getSampleStyleSheet
from typing import List

class Reporte_Asignaciones:
    def __init__(self, db_path="./centro_control.db"):
        self.db_path = db_path

    def obtener_asignaciones(self, fecha_inicio, fecha_fin, cedula=None, nombre=None, turno=None, concesion=None, control=None, ruta=None, linea=None, cop=None, registrado_por=None, nombre_supervisor_enlace=None):
        print(f"Filtros recibidos: {fecha_inicio}, {fecha_fin}, {cedula}, {nombre}, {turno}, {concesion}, {control}, {ruta}, {linea}, {cop}, {registrado_por}, {nombre_supervisor_enlace}")
        query = """
            SELECT fecha, cedula, nombre, turno, h_inicio, h_fin, concesion, control, ruta, linea, cop, observaciones, registrado_por, fecha_hora_registro, cedula_enlace, nombre_supervisor_enlace
            FROM asignaciones
            WHERE fecha BETWEEN ? AND ?
        """
        params = [fecha_inicio, fecha_fin]

        if cedula:
            query += " AND cedula = ?"
            params.append(cedula)
        if nombre:
            query += " AND nombre = ?"
            params.append(nombre)
        if turno:
            query += " AND turno = ?"
            params.append(turno)
        if concesion:
            query += " AND concesion = ?"
            params.append(concesion)
        if control:
            query += " AND control = ?"
            params.append(control)
        if ruta:
            query += " AND ruta = ?"
            params.append(ruta)
        if linea:
            query += " AND linea = ?"
            params.append(linea)
        if cop:
            query += " AND cop = ?"
            params.append(cop)
        if registrado_por:
            query += " AND registrado_por = ?"
            params.append(registrado_por)
        if nombre_supervisor_enlace:
            query += " AND nombre_supervisor_enlace = ?"
            params.append(nombre_supervisor_enlace)

        query += " ORDER BY fecha ASC"

        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute(query, tuple(params))
            resultados = cursor.fetchall()
            conn.close()
            
            # Transformar las filas en diccionarios
            resultado = []
            for row in resultados:
                resultado.append({
                    'fecha': row[0],
                    'cedula': row[1],
                    'nombre': row[2],
                    'turno': row[3],
                    'h_inicio': row[4],
                    'h_fin': row[5],
                    'concesion': row[6],
                    'control': row[7],
                    'ruta': row[8],
                    'linea': row[9],
                    'cop': row[10],
                    'observaciones': row[11],
                    'registrado_por': row[12],
                    'fecha_hora_registro': row[13],
                    'cedula_enlace': row[14],
                    'nombre_supervisor_enlace': row[15],
                })
            
            return resultado

        except sqlite3.Error as e:
            print(f"Error al consultar la base de datos: {e}")
            return []

    def obtener_filtros_unicos(self):
        filtros = {
            "cedulas": [],
            "nombres": [],
            "turnos": [],
            "concesiones": [],
            "controles": [],
            "rutas": [],
            "lineas": [],
            "cops": [],
            "registrado": [],
            "supervisores_enlace": []
        }

        query_templates = {
            "cedulas": "SELECT DISTINCT cedula FROM asignaciones ORDER BY cedula",
            "nombres": "SELECT DISTINCT nombre FROM asignaciones ORDER BY nombre",
            "turnos": "SELECT DISTINCT turno FROM asignaciones ORDER BY turno",
            "concesiones": "SELECT DISTINCT concesion FROM asignaciones ORDER BY concesion",
            "controles": "SELECT DISTINCT control FROM asignaciones ORDER BY control",
            "rutas": "SELECT DISTINCT ruta FROM asignaciones ORDER BY ruta",
            "lineas": "SELECT DISTINCT linea FROM asignaciones ORDER BY linea",
            "cops": "SELECT DISTINCT cop FROM asignaciones ORDER BY cop",
            "registrado": "SELECT DISTINCT registrado_por FROM asignaciones ORDER BY registrado_por",
            "supervisores_enlace": "SELECT DISTINCT nombre_supervisor_enlace FROM asignaciones ORDER BY nombre_supervisor_enlace"
        }

        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            for key, query in query_templates.items():
                cursor.execute(query)
                filtros[key] = [row[0] for row in cursor.fetchall()]
            conn.close()
        except sqlite3.Error as e:
            print(f"Error al consultar los filtros únicos: {e}")

        return filtros

    def generar_xlsx(self, filtros):
        asignaciones = self.obtener_asignaciones(
            filtros.get('fechaInicio'),
            filtros.get('fechaFin'),
            filtros.get('cedulaTecnico'),
            filtros.get('nombreTecnico'),
            filtros.get('turno'),
            filtros.get('concesion'),
            filtros.get('control'),
            filtros.get('ruta'),
            filtros.get('linea'),
            filtros.get('cop'),
            filtros.get('usuarioRegistra'),
            filtros.get('nombreSupervisorEnlace')
        )
        
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = "Asignaciones"
        
        headers = ["Fecha", "Cédula", "Nombre Técnico", "Turno", "Hora Inicio", "Hora Fin", "Concesión", "Control", "Ruta", "Línea", "COP", "Observaciones", "Usuario Registra", "Fecha de Registro", "Cedula Enlace", "Nombre Supervisor Enlace"]
        ws.append(headers)
        
        for asignacion in asignaciones:
            ws.append([
                asignacion['fecha'],
                asignacion['cedula'],
                asignacion['nombre'],
                asignacion['turno'],
                asignacion['h_inicio'],
                asignacion['h_fin'],
                asignacion['concesion'],
                asignacion['control'],
                asignacion['ruta'],
                asignacion['linea'],
                asignacion['cop'],
                asignacion['observaciones'],
                asignacion['registrado_por'],
                asignacion['fecha_hora_registro'],
                asignacion['cedula_enlace'],
                asignacion['nombre_supervisor_enlace']
            ])
        
        stream = BytesIO()
        wb.save(stream)
        stream.seek(0)
        
        return stream
    
    def generar_csv(self, filtros):
        asignaciones = self.obtener_asignaciones(
            filtros.get('fechaInicio'),
            filtros.get('fechaFin'),
            filtros.get('cedulaTecnico'),
            filtros.get('nombreTecnico'),
            filtros.get('turno'),
            filtros.get('concesion'),
            filtros.get('control'),
            filtros.get('ruta'),
            filtros.get('linea'),
            filtros.get('cop'),
            filtros.get('usuarioRegistra'),
            filtros.get('nombreSupervisorEnlace')
        )
        
        output = StringIO()
        writer = csv.writer(output)
        
        headers = ["Fecha", "Cédula", "Nombre Técnico", "Turno", "Hora Inicio", "Hora Fin", "Concesión", "Control", "Ruta", "Línea", "COP", "Observaciones", "Usuario Registra", "Fecha de Registro", "Cedula Enlace", "Nombre Supervisor Enlace"]
        writer.writerow(headers)
        
        for asignacion in asignaciones:
            writer.writerow([
                asignacion['fecha'],
                asignacion['cedula'],
                asignacion['nombre'],
                asignacion['turno'],
                asignacion['h_inicio'],
                asignacion['h_fin'],
                asignacion['concesion'],
                asignacion['control'],
                asignacion['ruta'],
                asignacion['linea'],
                asignacion['cop'],
                asignacion['observaciones'],
                asignacion['registrado_por'],
                asignacion['fecha_hora_registro'],
                asignacion['cedula_enlace'],
                asignacion['nombre_supervisor_enlace']
            ])
        
        output.seek(0)
        return output

    def generar_json(self, filtros):
        asignaciones = self.obtener_asignaciones(
            filtros.get('fechaInicio'),
            filtros.get('fechaFin'),
            filtros.get('cedulaTecnico'),
            filtros.get('nombreTecnico'),
            filtros.get('turno'),
            filtros.get('concesion'),
            filtros.get('control'),
            filtros.get('ruta'),
            filtros.get('linea'),
            filtros.get('cop'),
            filtros.get('usuarioRegistra'),
            filtros.get('nombreSupervisorEnlace')
        )
        
        return json.dumps(asignaciones, ensure_ascii=False, indent=4)

###### FUNCIONES PARA CONSULTAR DE LA BASE DE DATOS Y TRAER LAS ASIGNACIONES AL FRONTEND ####### 
    def obtener_asignacion_por_fecha(self, fecha, concesion, fecha_hora_registro):
        query = """
            SELECT fecha, cedula, nombre, turno, h_inicio, h_fin, concesion, control, 
                GROUP_CONCAT(ruta), linea, cop, observaciones, puestosSC, puestosUQ, 
                fecha_hora_registro
            FROM asignaciones
            WHERE DATE(fecha) = ? AND concesion = ? AND fecha_hora_registro = ?
            GROUP BY cedula, nombre, turno, h_inicio, h_fin, concesion, control, linea, cop, 
                    observaciones, puestosSC, puestosUQ
            ORDER BY fecha_hora_registro DESC
        """
        params = (fecha, concesion, fecha_hora_registro)

        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute(query, params)
            resultados = cursor.fetchall()
            conn.close()

            # Devolver una lista de asignaciones
            asignaciones = []
            for resultado in resultados:
                asignaciones.append({
                    'fecha': resultado[0],
                    'cedula': resultado[1],
                    'nombre': resultado[2],
                    'turno': resultado[3],
                    'h_inicio': resultado[4],
                    'h_fin': resultado[5],
                    'concesion': resultado[6],
                    'control': resultado[7],
                    'ruta': resultado[8],
                    'linea': resultado[9],
                    'cop': resultado[10],
                    'observaciones': resultado[11],
                    'puestosSC': resultado[12],
                    'puestosUQ': resultado[13],
                    'fecha_hora_registro': resultado[14]
                })

            return asignaciones  # Devuelve una lista de asignaciones
        except sqlite3.Error as e:
            print(f"Error al consultar la base de datos: {e}")
            return None

    def obtener_concesiones_unicas_por_fecha(self, fecha):
        query = """
            SELECT DISTINCT concesion
            FROM asignaciones
            WHERE fecha = ?
        """
        params = (fecha,)

        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute(query, params)
            concesiones = cursor.fetchall()
            conn.close()

            if concesiones:
                return [concesion[0] for concesion in concesiones]  # Devolver una lista de concesiones únicas
            return None

        except sqlite3.Error as e:
            print(f"Error al consultar la base de datos: {e}")
            return None

    def obtener_fechas_horas_registro(self, fecha, concesion):
        query = """
            SELECT DISTINCT fecha_hora_registro
            FROM asignaciones
            WHERE DATE(fecha) = ? AND concesion = ?
            ORDER BY fecha_hora_registro DESC
        """
        params = (fecha, concesion)

        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute(query, params)
            resultados = cursor.fetchall()
            conn.close()

            fechas_horas = [resultado[0] for resultado in resultados]
            return fechas_horas  # Devolver solo las fechas y horas únicas
        except sqlite3.Error as e:
            print(f"Error al consultar la base de datos: {e}")
            return None

###### FUNCIONES PARA CONSULTAR DE LA BASE DE DATOS Y TRAER LAS ASIGNACIONES EN PDF ####### 
    def generar_pdf(self, asignaciones, fecha_asignacion, fecha_hora_registro):
        # Ruta del archivo PDF 
        pdf_file = "asignaciones_tecnicos.pdf"

        # Crear el documento PDF
        doc = SimpleDocTemplate(pdf_file, pagesize=A4)
        elements = []
        styles = getSampleStyleSheet()

        # Añadir título y subtítulo
        titulo = Paragraph(f"Informe de Asignaciones Centro de Control para el {fecha_asignacion}", styles['Title'])
        subtitulo = Paragraph(f"Actualización: {fecha_hora_registro}", styles['Normal'])
        elements.append(titulo)
        elements.append(subtitulo)
        elements.append(Spacer(1, 12))

        # Agrupar asignaciones por técnico
        asignaciones_por_tecnico = {}
        for asignacion in asignaciones:
            nombre_tecnico = asignacion.nombre
            if nombre_tecnico not in asignaciones_por_tecnico:
                asignaciones_por_tecnico[nombre_tecnico] = {
                    'datos_tecnico': asignacion,
                    'rutas': []
                }
            asignaciones_por_tecnico[nombre_tecnico]['rutas'].append((asignacion.ruta, asignacion.cop, asignacion.linea))

        # Dividir técnicos en grupos de 2 para hacer una distribución en columnas
        tecnicos = list(asignaciones_por_tecnico.items())
        filas = []
        for i in range(0, len(tecnicos), 2):
            fila = tecnicos[i:i+2]
            fila_tablas = []
            for nombre_tecnico, datos_tecnico in fila:
                # Información del técnico y su tabla de rutas en una sola columna
                tecnico_info = Paragraph(
                    f"<b>Técnico:</b> {nombre_tecnico}<br/><b>Cédula:</b> {datos_tecnico['datos_tecnico'].cedula}<br/>"
                    f"<b>Puesto de Trabajo:</b> {datos_tecnico['datos_tecnico'].control}<br/><b>Turno:</b> {datos_tecnico['datos_tecnico'].turno}",
                    styles['Normal']
                )
                elements.append(tecnico_info)
                elements.append(Spacer(1, 6))

                # Crear una tabla con las rutas del técnico
                data = [['Ruta', 'COP', 'Línea']]
                for ruta, cop, linea in datos_tecnico['rutas']:
                    data.append([ruta, cop, linea])

                # Crear la tabla de rutas con tamaño ajustado y color de encabezado azul oscuro
                table = Table(data, colWidths=[0.7 * inch, 1 * inch, 0.7 * inch])
                table.setStyle(TableStyle([
                    ('BACKGROUND', (0, 0), (-1, 0), colors.darkblue),
                    ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                    ('ALIGN', (0, 0), (-1, -1), 'CENTER'),  
                    ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                    ('FONTSIZE', (0, 0), (-1, -1), 8),
                    ('TOPPADDING', (0, 0), (-1, -1), 2),  # Ajusta el espacio en la parte superior de cada celda (padding superior).
                    ('BOTTOMPADDING', (0, 0), (-1, 0), 2), #Ajusta el espacio en la parte inferior de cada celda (padding inferior).
                    ('BACKGROUND', (0, 1), (-1, -1), colors.white),
                    ('GRID', (0, 0), (-1, -1), 1, colors.black),
                ]))
                elements.append(table)
                elements.append(Spacer(1, 12))  # Espacio entre técnicos

            # Añadir un espaciado entre cada grupo de dos técnicos
            elements.append(Spacer(1, 24))

        # Generar el PDF
        doc.build(elements)
        return pdf_file