
FROM apache/airflow:latest

USER root

# Install required packages
RUN apt-get update && \
    apt-get -y install git unixodbc-dev && \
    apt-get clean

# Download and install the Microsoft ODBC Driver for SQL Server
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - && \
    curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list && \
    apt-get update && \
    ACCEPT_EULA=Y apt-get -y install msodbcsql17 && \
    apt-get clean

# Set permissions on specific directories
RUN chmod -R 777 /opt/airflow

USER airflow



FROM apache/airflow:latest

USER root

# Install required packages
RUN apt-get update && \
    apt-get -y install git unixodbc-dev && \
    apt-get clean

# Download and install the Microsoft ODBC Driver for SQL Server
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - && \
    curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list && \
    apt-get update && \
    ACCEPT_EULA=Y apt-get -y install msodbcsql17 && \
    apt-get clean

# Set permissions on specific directories
RUN chmod -R 777 /opt/airflow

# Set environment variables for SQL Server connection
ENV AIRFLOW_CONN_MSSQL_SERVER mssql+pyodbc://airbnb_analyst:PkpdndMSgUluU0d@192.168.56.1/Airbnb_DW?driver=ODBC+Driver+18+for+SQL+Server

# Modify the airflow.cfg file to include SQL Server connection details
RUN echo "sql_alchemy_conn = ${AIRFLOW_CONN_MSSQL_SERVER}" >> /opt/airflow/airflow.cfg

USER airflow

# Install SQLAlchemy with a specific version
RUN pip install geopy
RUN pip install pymongo

