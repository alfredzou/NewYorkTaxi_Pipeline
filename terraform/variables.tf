# Credentials
variable "gcs_bq_credentials" {
    description = "My Credential Location"
    default = "~/.gcp/gcs_bq.json"
}

# Change these
variable "Project" {
    description = "My Project Name"
    default = "newyorktaxi-423104" 
}

variable "bucket_name" {
    description = "My Storage Bucket Name"
    default = "newyorktaxi-423104-bucket1"
}

# Keep default
variable "region" {
    description = "My Project Region"
    default = "australia-southeast2" 
}