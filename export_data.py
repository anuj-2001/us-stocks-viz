import pandas as pd
from fpdf import FPDF

# Function to export data to Excel
def export_to_excel(historical_index_data, filename="index_performance.xlsx"):
    df = pd.DataFrame(historical_index_data)
    df.to_excel(filename, index=False)
    print(f"Data exported to {filename}")

# Function to export data to PDF
def export_to_pdf(historical_index_data, filename="index_performance.pdf"):
    c = canvas.Canvas(filename, pagesize=letter)
    c.setFont("Helvetica", 12)

    y = 750
    for entry in historical_index_data:
        c.drawString(50, y, f"Date: {entry['Date']}")
        c.drawString(50, y - 20, f"Index Return: {entry['IndexReturn']}")
        c.drawString(50, y - 40, f"Top 100 Stocks: {', '.join(entry['Top100Stocks'])}")
        y -= 60
        if y < 50:
            c.showPage()
            y = 750

    c.save()
    print(f"Data exported to {filename}")