FROM python:latest
ADD main.py .
ADD weather.py .
RUN pip install flask numpy requests datetime pandas
CMD ["python", "./main.py"] 