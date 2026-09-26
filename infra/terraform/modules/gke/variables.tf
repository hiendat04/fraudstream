variable "project_id" {
  type = string
}

variable "name" {
  type = string
}

variable "zone" {
  type = string
}

variable "network_id" {
  type = string
}

variable "subnet_id" {
  type = string
}

variable "pods_range_name" {
  type = string
}

variable "services_range_name" {
  type = string
}

variable "machine_type" {
  type    = string
  default = "e2-standard-2"
}

variable "node_count" {
  type    = number
  default = 2
}

variable "spot" {
  type    = bool
  default = false
}
