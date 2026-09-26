variable "project_id" {
  type = string
}

variable "name" {
  type = string
}

variable "zone" {
  type = string
}

variable "network_name" {
  type = string
}

variable "subnet_id" {
  type = string
}

variable "admin_cidr" {
  type = string
}

variable "ssh_user" {
  type = string
}

variable "ssh_public_key" {
  type = string
}

variable "machine_type" {
  type    = string
  default = "e2-medium"
}

variable "client_cidrs" {
  type = list(string)
}

variable "service_ports" {
  type = list(string)client_cidrs
}
