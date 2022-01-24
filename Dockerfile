FROM python:3.9-slim

# Copy function code
COPY fargate_function.py .

# Install the function's dependencies using file requirements.txt
COPY requirements.txt  .
RUN  pip3 install -r requirements.txt

# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
CMD [ "python", "fargate_function.py" ]
