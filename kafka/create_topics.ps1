
$kafka = "cdc-kafka"

$topics = @(
    "telecom.crm.customers",
    "telecom.crm.devices",
    "telecom.crm.plans",
    "telecom.billing.accounts",
    "telecom.billing.invoices",
    "telecom.billing.invoice_lines",
    "telecom.billing.payments"
)

foreach ($topic in $topics) {
    docker exec $kafka /opt/kafka/bin/kafka-topics.sh `
        --bootstrap-server kafka:29092 `
        --create `
        --if-not-exists `
        --topic $topic `
        --partitions 1 `
        --replication-factor 1
}

docker exec $kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --list