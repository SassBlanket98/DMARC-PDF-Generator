from datetime import datetime, timedelta
from elasticsearch import Elasticsearch
from fpdf import FPDF
import matplotlib.pyplot as plt
from io import BytesIO
import geopandas as gpd
from shapely.geometry import Point
from mpl_toolkits.axes_grid1 import make_axes_locatable
import pycountry
import tempfile
import os

# Connect to Elasticsearch
es = Elasticsearch(hosts=["http://localhost:9200"])

# Function to collect records based on date range and domain
def collect_records(index_pattern, days, domain):
    try:
        # Calculate the date range
        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=days)
        start_date_str = int(start_date.timestamp() * 1000)  # epoch millis
        end_date_str = int(end_date.timestamp() * 1000)  # epoch millis

        # Query to filter records
        query = {
            "size": 10000,  # Adjust the size as needed
            "query": {
                "bool": {
                    "must": [
                        {
                            "range": {
                                "date_begin": {
                                    "gte": start_date_str,
                                    "lte": end_date_str
                                }
                            }
                        },
                        {
                            "term": {
                                "published_policy.domain.keyword": domain
                            }
                        }
                    ]
                }
            }
        }

        # Search across indices
        response = es.search(index=index_pattern, body=query)
        records = [hit["_source"] for hit in response["hits"]["hits"]]
        return records
    except Exception as e:
        print(f"Error collecting records: {e}")
        return []

def convert_country_codes_to_names(country_counts):
    country_name_mapping = {}
    for country_code in country_counts.keys():
        country = pycountry.countries.get(alpha_2=country_code)
        if country:
            country_name_mapping[country_code] = country.name
        else:
            print(f"Warning: Country code {country_code} not recognized")
    return {country_name_mapping.get(k, k): v for k, v in country_counts.items()}

def generate_world_map(country_counts):
    # Load the world map dataset from the local file
    world = gpd.read_file("data/ne_110m_admin_0_countries.shp")

    # Convert country codes to country names
    normalized_country_counts = convert_country_codes_to_names(country_counts)

    # Create a mapping of country names to the names used in the shapefile
    country_name_mapping = {
        "United States": "United States of America",
        "Russia": "Russian Federation",
        "South Korea": "Korea, Republic of",
        "North Korea": "Korea, Democratic People's Republic of",
        "Iran": "Iran, Islamic Republic of",
        "Syria": "Syrian Arab Republic",
        "Venezuela": "Venezuela, Bolivarian Republic of",
        "Bolivia": "Bolivia, Plurinational State of",
        "Moldova": "Moldova, Republic of",
        "Macedonia": "North Macedonia",
        "Vietnam": "Viet Nam",
        "Laos": "Lao People's Democratic Republic",
        "Brunei": "Brunei Darussalam",
        "Tanzania": "Tanzania, United Republic of",
        "Congo": "Congo, Republic of the",
        "Ivory Coast": "Côte d'Ivoire",
        "Cape Verde": "Cabo Verde",
        "East Timor": "Timor-Leste",
    }

    # Create a GeoDataFrame for the countries with message counts
    geometry = []
    sizes = []

    for country, count in normalized_country_counts.items():
        # Use the mapping to get the correct country name
        shapefile_country = country_name_mapping.get(country, country)
        matching_countries = world[world["ADMIN"] == shapefile_country]
        if not matching_countries.empty:
            geometry.append(matching_countries.geometry.iloc[0].centroid)
            sizes.append(count)
        else:
            print(f"Country not found in shapefile: {country}")

    if not geometry:
        print("No matching countries found for circles.")
        return None

    geo_df = gpd.GeoDataFrame({"size": sizes}, geometry=geometry)

    # Plot the world map and overlay circles
    fig, ax = plt.subplots(figsize=(15, 10))
    world.plot(ax=ax, color="lightgrey")

    if not geo_df.empty:
        # Adjust the scaling factor for circle sizes
        max_size = 5000  # Set a maximum size for the circles
        min_size = 50   # Set a minimum size for the circles
        scale_factor = 5  # Adjust this factor to scale the sizes appropriately
        scaled_sizes = [max(min_size, min(max_size, size * scale_factor)) for size in geo_df["size"]]
        geo_df.plot(ax=ax, markersize=scaled_sizes, color="red", alpha=0.6)

    ax.set_title("Messages by Country", fontsize=16)

    # Save to a BytesIO buffer
    buffer = BytesIO()
    plt.savefig(buffer, format="png", bbox_inches="tight")
    buffer.seek(0)
    plt.close(fig)
    return buffer

def generate_pdf(records, domain, output_folder="results"):
    # Ensure the output folder exists
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    # Construct the output file name and full path
    sanitized_domain = domain.replace('.', '_')  # Replace dots in the domain for file naming
    output_file = f"dmarc_report_{sanitized_domain}.pdf"
    full_path = os.path.join(output_folder, output_file)

    pdf = FPDF()
    pdf.set_font("Arial", size=12)
    pdf.add_page()

    # Add logo to the top left
    logo_path = "Images/logo_non_interlaced.png"  # Update with the actual path to your logo
    pdf.image(logo_path, x=5, y=5, w=60)  # Adjust x, y, and w as needed

    # Add contact details to the top right
    pdf.set_xy(5, 5)  # Adjust x and y as needed
    pdf.set_font("Arial", size=10)
    pdf.multi_cell(0, 5, "Contact Details:\nName: David Hill\nEmail: david@neozeit.com\nPhone: 061-058-4433", align="R")
    

    # Title
    pdf.set_xy(10, 25)  # Adjust y as needed to position below the logo and contact details
    pdf.set_font("Arial", style="BU", size=16)
    pdf.cell(200, 10, txt=f"DMARC Report Summary for: {domain}", ln=True, align="C")
    pdf.ln(5)

    # General Summary Section
    pdf.set_font("Arial", style="BU", size=12)
    pdf.cell(200, 7.5, txt="General Overview", ln=True)
    pdf.set_font("Arial", size=10)

    # Calculate totals correctly
    total_messages = sum(record.get("message_count", 0) for record in records)
    total_dkim_aligned = sum(record.get("message_count", 0) for record in records if record.get("dkim_aligned", False))
    total_spf_passed = sum(record.get("message_count", 0) for record in records if record.get("spf_aligned", False))
    total_dmarc_passed = sum(record.get("message_count", 0) for record in records if record.get("passed_dmarc", False))
    total_dkim_unaligned = sum(record.get("message_count", 0) for record in records if not record.get("dkim_aligned", False))
    total_spf_failed = sum(record.get("message_count", 0) for record in records if not record.get("spf_aligned", False))
    total_dmarc_failed = sum(record.get("message_count", 0) for record in records if not record.get("passed_dmarc", False))

    # Display summary
    pdf.multi_cell(0, 7.5, txt=f"Total Messages from: {total_messages}\n"
                              f"Total DKIM aligned: {total_dkim_aligned}"
                              " : "
                              f"Total DKIM unaligned: {total_dkim_unaligned}\n"
                              f"Total SPF Passed: {total_spf_passed}"
                              " : "
                              f"Total SPF Failed: {total_spf_failed}\n"
                              f"Total DMARC Passed: {total_dmarc_passed}"
                              " : "
                              f"Total DMARC Failed: {total_dmarc_failed}\n")
    pdf.ln(5)

    # Messages by Country Section
    pdf.set_font("Arial", style="BU", size=12)
    pdf.cell(200, 7.5, txt="Messages by Country", ln=True)
    pdf.set_font("Arial", size=10)
    country_counts = {}
    for record in records:
        country = record.get("source_country", "Unknown")
        country_counts[country] = country_counts.get(country, 0) + record.get("message_count", 0)

    # Sort the country_counts dictionary by message count in descending order
    sorted_country_counts = sorted(country_counts.items(), key=lambda item: item[1], reverse=True)

    for country, count in sorted_country_counts:
        pdf.cell(0, 7.5, txt=f"{country}: {count}", ln=True)
    pdf.ln(-5)

    # Add the map visualization directly below the Messages by Country section
    map_buffer = generate_world_map(country_counts)
    if map_buffer:
        with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as temp_file:
            temp_file.write(map_buffer.getvalue())
            temp_file.flush()
            pdf.image(temp_file.name, x=10, y=pdf.get_y() + 10, w=190)  # Adjust the placement and size as needed

        # Add a new page for Reporting Organizations
        pdf.add_page()
        pdf.set_font("Arial", style="BU", size=12)
        pdf.cell(200, 10, txt="Reporting Organizations", ln=True)
        pdf.set_font("Arial", size=10)

        # Reporting Organizations Section
        org_data = {}
        for record in records:
            org_name = record.get("org_name", "Unknown")
            org_email = record.get("org_email", "Unknown")
            message_count = record.get("message_count", 0)

            if org_name not in org_data:
                org_data[org_name] = {"email": org_email, "messages": 0}
            org_data[org_name]["messages"] += message_count

        # Sort the org_data dictionary by message count in descending order
        sorted_org_data = sorted(org_data.items(), key=lambda item: item[1]["messages"], reverse=True)

        for org_name, details in sorted_org_data:
            pdf.multi_cell(0, 10, txt=f"Organization: {org_name}\n"
                                    f"Contact: {details['email']}\n"
                                    f"Messages: {details['messages']}\n")
            
            # Draw a line underneath each result
            line_y = pdf.get_y()
            pdf.line(10, line_y, 70, line_y)
            # pdf.ln(5)
            
            

    # Save the PDF
    pdf.output(full_path)
    print(f"PDF report generated as '{full_path}'.")

# Usage example
if __name__ == "__main__":
    index_pattern = "dmarc_aggregate-*"  # Use a specific index pattern if necessary
    days = 31 # Specify the number of days to search
    domain = input("Please enter the domain you wish to summarize: ")  # Replace with the specific domain

    records = collect_records(index_pattern, days, domain)

    if records:
        print(f"Collected {len(records)} records for domain '{domain}' in the past {days} days.")
        for record in records:
            print(record)

        # Generate the PDF report
        generate_pdf(records, domain)
    else:
        print("No records found.")
