import pandas as pd
from fpdf import FPDF

def export_data_to_excel(index_data, file_name):
    index_data.to_excel(file_name, index=False)

def export_data_to_pdf(index_data, file_name):
    pdf = FPDF()
    pdf.add_page()
    pdf.set_font("Arial", size=12)
    for row in index_data.itertuples():
        pdf.cell(200, 10, txt=str(row), ln=True, align='L')
    pdf.output(file_name)
