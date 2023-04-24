export $(cat .env | xargs)

terraform init
terraform plan -var="project=$PROJECT_ID"
terraform apply -var="project=$PROJECT_ID"