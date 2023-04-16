export GOOGLE_APPLICATION_CREDENTIALS="/home/roman/Загрузки/sacred-alloy-375819-e7268563e1e4.json"
terraform init
terraform plan -var="project=sacred-alloy-375819"
terraform apply -var="project=sacred-alloy-375819"
prefect cloud login -k pnu_iKNmGcmslZs8wbN5xHXYwoRLAQPOF14GlPT3
