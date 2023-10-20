import smtplib
from email.message import EmailMessage
import json
import os

def sendErrorEmail(context):
    dag_run = context.get("dag_run")
    failedInstances = dag_run.get_task_instances(state="failed")
    fistFailedId = failedInstances[0]
    emailHtml = htmlErrorBuilder( fistFailedId.start_date, fistFailedId.task_id, fistFailedId.dag_id)
    subject = 'Airflow DAG Error'
    sendEmail(emailHtml, subject)

def sendAlertEmail(exec_date, dag_path):
    emailHtml = htmlAlertBuilder(exec_date, dag_path)
    subject = 'Airflow Alerts'
    return sendEmail(emailHtml, subject)
    

def sendEmail(emailHtml, subject):
    try:
        
        message = EmailMessage()
        message["From"] = os.getenv("EMAIL_USER")
        message["To"] = os.getenv("EMAIL_USER")
        message["Subject"] = subject
        message.set_content(emailHtml, 'html')

        with smtplib.SMTP_SSL(os.getenv("SMTP_HOST"), 465) as server:
            server.login(os.getenv("EMAIL_USER"), os.getenv("EMAIL_PASSWORD"))
            server.send_message(message)

        print('Exito')
        return True
    except Exception as exception:
        print(exception)
        print('Failure')
        return False

def htmlAlertBuilder(exec_date, dag_path):

    with open(dag_path+'/error_data/'+"data_"+exec_date+".json", "r") as json_file:
        alertsData=json.load(json_file)
        print(f""" Se obtienen las alertas desde {dag_path}+'/processed_data/'+"data_"+{exec_date}+".json \n""")

    tableCols = ''
    for alertData in alertsData:
        tableCols += f"""<tr>
            <td style="border: 1px solid black; padding: 8px; text-align: center;">{alertData['propertyId']}</td>
            <td style="border: 1px solid black; padding: 8px; text-align: center;">{alertData['variableName']}</td>
            <td style="border: 1px solid black; padding: 8px; text-align: center;">{alertData['variableValue']}</td>
            <td style="border: 1px solid black; padding: 8px; text-align: center;">{alertData['variableTreshold']}</td>
            <td style="border: 1px solid black; padding: 8px; text-align: center;">{alertData['propertyDate']}</td>
        </tr>"""

    htmlCode = f"""
         <!DOCTYPE html>
            <html>
            <head>
                <meta charset="UTF-8">
                <title>Alertas Airflow</title>
            </head>
            <body style="margin: 0; padding: 0; font-family: Arial, sans-serif; background-color: #f4f4f4;">
                <table role="presentation" width="100%" cellspacing="0" cellpadding="0">
                    <tr>
                        <td align="center">
                            <table width="600" cellpadding="0" cellspacing="0" style="background-color: #ffffff; box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1); border-radius: 6px; margin: 20px;">
                                <!-- Header -->
                                <tr>
                                    <td style="padding: 20px;">
                                        <h1 style="color: #333333; font-size: 24px;">Alertas Airflow: Realtor DAG</h1>
                                    </td>
                                </tr>
                                <!-- Email Content -->
                                <tr>
                                    <td style="padding: 20px;">
                                        <p style="color: #555555; font-size: 16px;">Este es un email de alerta que contiene uno o varios datos de la API de Realtor que no cumplen con los criterios establecidos y que fueron consultados el <b>{exec_date}</b>.</p>
                                        <p style="color: #923030; font-size: 16px;"><strong>Alerta: Las siguientes propiedades no cumplen con los limites para precio, ciudad o tipo de propiedad.</strong></p>
                                        <table style="border-collapse: collapse; width: 100%;">
                                            <thead>
                                                <tr>
                                                    <th style="border: 1px solid black; background-color: #f2f2f2; padding: 8px; text-align: center;">ID propiedad</th>
                                                    <th style="border: 1px solid black; background-color: #f2f2f2; padding: 8px; text-align: center;">Variable errónea</th>
                                                    <th style="border: 1px solid black; background-color: #f2f2f2; padding: 8px; text-align: center;">Valor recibido</th>
                                                    <th style="border: 1px solid black; background-color: #f2f2f2; padding: 8px; text-align: center;">Valores aceptados</th>
                                                    <th style="border: 1px solid black; background-color: #f2f2f2; padding: 8px; text-align: center;">Fecha publicación propiedad</th>
                                                </tr>
                                            </thead>
                                            <tbody>
                                                {tableCols}
                                            </tbody>
                                        </table>
                                    </td>
                                </tr>
                                <!-- Footer -->
                                <tr>
                                    <td style="background-color: #333333; color: #ffffff; text-align: center; padding: 10px 0;">
                                        &copy; 2023 Realtor API. Un proyecto para Coderhouse.
                                    </td>
                                </tr>
                            </table>
                        </td>
                    </tr>
                </table>
            </body>
            </html>
    """
    return htmlCode

def htmlErrorBuilder(startDate, taskId, dagId):
    htmlCode = f"""
         <!DOCTYPE html>
            <html>
            <head>
                <meta charset="UTF-8">
                <title>Errores Airflow</title>
            </head>
            <body style="margin: 0; padding: 0; font-family: Arial, sans-serif; background-color: #f4f4f4;">
                <table role="presentation" width="100%" cellspacing="0" cellpadding="0">
                    <tr>
                        <td align="center">
                            <table width="600" cellpadding="0" cellspacing="0" style="background-color: #ffffff; box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1); border-radius: 6px; margin: 20px;">
                                <!-- Header -->
                                <tr>
                                    <td style="padding: 20px;">
                                        <h1 style="color: #333333; font-size: 24px;">Errores Airflow: Realtor DAG</h1>
                                    </td>
                                </tr>
                                <!-- Email Content -->
                                <tr>
                                    <td style="padding: 20px;">
                                        <p style="color: #555555; font-size: 16px;"></p>
                                        <p style="color: #923030; font-size: 16px;"><strong>Alerta: Este es un email informativo indicando que el DAG: {dagId} ha fallado en el task: {taskId}, favor tomar acciones. La fecha de error fue: <b>{startDate}</b>.</strong></p>
                                    </td>
                                </tr>
                                <!-- Footer -->
                                <tr>
                                    <td style="background-color: #333333; color: #ffffff; text-align: center; padding: 10px 0;">
                                        &copy; 2023 Realtor API. Un proyecto para Coderhouse.
                                    </td>
                                </tr>
                            </table>
                        </td>
                    </tr>
                </table>
            </body>
            </html>
    """
    return htmlCode
