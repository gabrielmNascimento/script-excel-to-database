import pandas as pd
import psycopg2
from psycopg2 import sql
import os
import dotenv

# Conectando ao banco de dados
def connect_to_db():
    IP_DATABASE = os.getenv('IP_DATABASE')
    USER = os.getenv('USER')
    PASSWORD = os.getenv('PASSWORD')
    DATABASE = os.getenv('DATABASE') 
    PORT = os.environ['DB_PORT']

    try:
        conn = psycopg2.connect("dbname='{}' user='{}' host='{}' password='{}' port='{}'".format(DATABASE,USER,IP_DATABASE,PASSWORD,PORT))
        return conn
    except Exception:
        print ("I am unable to connect to the database")
        exit(0)
        return None
    #cur = conn.cursor(cursor_factory = psycopg2.extras.RealDictCursor)
    
# Atualizando o banco de dados
def update_database(conn, df):
    cur = conn.cursor()
    
    for index, row in df.iterrows():

        # Atualizar ronald.ronalds
        if 'Donate' in row and pd.notna(row['Donate']):
            try:
                # Attempt to parse the date
                date_value = pd.to_datetime(row['Donate']).date()
                
                query = sql.SQL("""
                    UPDATE ronald.ronalds
                    SET donate = %s
                    WHERE id = %s
                """)
                cur.execute(query, (date_value, row['ID Ronald']))
            except ValueError:
                print(f"Error: Donate inválida na linha {index + 2}. Pulando essa atualização.")
                continue

        # Atualizar ronald.entrances
        entrances_updates = []
        entrances_values = []

        if 'Ostra' in row and pd.notna(row['Ostra']):
            ostra = str(row['Ostra'])
            if len(ostra) > 200:
                print(f"Warning: 'Ostra' na linha {index + 2} excede 200 caracteres. Truncando.")
                ostra = ostra[:200]
            entrances_updates.append(sql.SQL("ostra = %s"))
            entrances_values.append(ostra)
        
        if 'Energia Ornamento' in row and pd.notna(row['Energia Ornamento']):
            energia_ornamento = str(row['Energia Ornamento'])
            if len(energia_ornamento) > 50:
                print(f"Warning: 'Energia Ornamento' na linha {index + 2} excede 50 caracteres. Truncando.")
                energia_ornamento = energia_ornamento[:50]
            entrances_updates.append(sql.SQL("energia_ornamento = %s"))
            entrances_values.append(energia_ornamento)
        
        if 'Energia Pimple' in row and pd.notna(row['Energia Pimple']):
            energia_pimple = row['Energia Pimple']
            if isinstance(energia_pimple, bool):
                entrances_updates.append(sql.SQL("energia_pimple = %s"))
                entrances_values.append(energia_pimple)
            else:
                print(f"Warning: 'Energia Pimple' na linha {index + 2} não é um valor booleano. Ignorando este campo.")
        
        if entrances_updates:
            query = sql.SQL("UPDATE ronald.entrances SET {} WHERE id = %s").format(
                sql.SQL(", ").join(entrances_updates)
            )
            cur.execute(query, entrances_values + [row['ID Entrance']])

        # Atualizar ronald.omars
        omars_updates = []
        omars_values = []

        if 'onesto Extremo' in row and pd.notna(row['onesto Extremo']):
            onesto_extremo = row['onesto Extremo']
            try:
                onesto_extremo = int(onesto_extremo)
                if len(str(onesto_extremo)) > 10:
                    print(f"Warning: 'onesto Extremo' na linha {index + 2} excede 10 caracteres. Ignorando este campo.")
                else:
                    omars_updates.append(sql.SQL("extremo = %s"))
                    omars_values.append(onesto_extremo)
            except ValueError:
                print(f"Warning: 'onesto Extremo' na linha {index + 2} não é um número inteiro válido. Ignorando este campo.")

        if 'Pinta otto' in row and pd.notna(row['Pinta otto']):
            pinta_otto = str(row['Pinta otto'])
            if len(pinta_otto) > 20:
                print(f"Warning: 'Pinta otto' na linha {index + 2} excede 20 caracteres. Truncando.")
                energia_ornamento = energia_ornamento[:20]
            omars_updates.append(sql.SQL("otto = %s"))
            omars_values.append(pinta_otto)
        
        if omars_updates:
            query = sql.SQL("UPDATE ronald.omars SET {} WHERE id = (SELECT omar_id FROM ronald.ronalds WHERE id = %s)").format(
                sql.SQL(", ").join(omars_updates)
            )
            cur.execute(query, omars_values + [row['ID Ronald']])

        # Atualizar public.peruanos
        peruanos_updates = []
        peruanos_values = []
        
        if 'Evidencia' in row and pd.notna(row['Evidencia']):
            evidencia = str(row['Evidencia'])
            if len(evidencia) > 220:
                print(f"Warning: 'Evidencia' na linha {index + 2} excede 220 caracteres. Truncando.")
                evidencia = evidencia[:220]
            peruanos_updates.append(sql.SQL("evidencia = %s"))
            peruanos_values.append(evidencia)

        if 'Bonito' in row and pd.notna(row['Bonito']):
            bonito = str(row['Bonito'])
            if len(bonito) > 220:
                print(f"Warning: 'Bonito' na linha {index + 2} excede 220 caracteres. Truncando.")
                bonito = bonito[:220]
            peruanos_updates.append(sql.SQL("bonito = %s"))
            peruanos_values.append(bonito)
        
        if peruanos_updates:
            query = sql.SQL("UPDATE public.peruanos SET {} WHERE id = (SELECT peruano_id FROM inerente.inerentes WHERE id = (SELECT inerente_id FROM ronald.ronalds WHERE id = %s))").format(
                sql.SQL(", ").join(peruanos_updates)
            )
            cur.execute(query, peruanos_values + [row['ID Ronald']])
    
    conn.commit()
    cur.close()

def get_excel_path(conn):
    cur = conn.cursor()
    try:
        cur.execute("SELECT path FROM tmp.forro ORDER BY id DESC LIMIT 1")
        result = cur.fetchone()
        if result:
            return result[0]
        else:
            print("Path não encontrado em tmp.forro")
            return None
    except Exception as e:
        erro = f"Error retrieving Excel file path from database: {e}"
        return None
    finally:
        cur.close()

def update_forro_status(conn, excel_file, verified=False, imported=False, error=False, message=None):
    cur = conn.cursor()
    try:
        query = sql.SQL("""
            UPDATE tmp.forro 
            SET verificado = %s, importado = %s, erro = %s, mensagem = %s
            WHERE path = %s
        """)
        cur.execute(query, (verified, imported, error, message, excel_file))
        conn.commit()
    except Exception as e:
        print(f"Error updating tmp.forro: {e}")
        conn.rollback()
    finally:
        cur.close()

def main():
    try:
        dotenv.load_dotenv()
    except:
        exit(0)

    PATH_UPLOAD = os.getenv('PATH_UPLOAD')
    
    conn = connect_to_db()
    if conn is None:
        return

    excel_file = get_excel_path(conn)
    if excel_file is None:
        conn.close()
        return

    try:
        df = pd.read_excel(PATH_UPLOAD + excel_file)
        # Verify if the necessary columns are present
        required_columns = ['ID Ronald', 'ID Entrance', 'Donate', 'Ostra', 'Energia Ornamento', 'Energia Pimple', 'onesto Extremo', 'Pinta otto', 'Evidencia', 'Bonito']
        if all(col in df.columns for col in required_columns):
            update_forro_status(conn, excel_file, verified=True, message="Verificado com sucesso")
        else:
            missing_columns = [col for col in required_columns if col not in df.columns]
            error_message = f"Colunas ausentes no arquivo: {', '.join(missing_columns)}"
            update_forro_status(conn, excel_file, verified=True, error=True, message=error_message)
            print(error_message)
            conn.close()
            return
    except Exception as e:
        error_message = f"Erro ao ler arquivo Excel: {e}"
        update_forro_status(conn, excel_file, error=True, message=error_message)
        print(error_message)
        conn.close()
        return

    try:
        update_database(conn, df)
        update_forro_status(conn, excel_file, imported=True, message="Importado sem Problemas")
        print("Banco de dados atualizado!")
    except Exception as e:
        error_message = f"Erro ao atualizar o banco de dados: {e}"
        update_forro_status(conn, excel_file, error=True, message=error_message)
        print(error_message)
    finally:
        conn.close()

if __name__ == "__main__":
    main()
