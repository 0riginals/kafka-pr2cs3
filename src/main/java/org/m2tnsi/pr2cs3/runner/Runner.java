package org.m2tnsi.pr2cs3.runner;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

@Component
public class Runner implements CommandLineRunner {

    private static final String KAFKA_BROKER = "localhost:9092";
    private static final String TOPIC_2 = "Topic2";
    private static final String TOPIC_3 = "Topic3";
    private static final String CONSUMER_ID_CONFIG = "covid19";
    private static final int MAX_POLL_RECORDS = 1;

    @Override
    public void run(String... args) throws Exception {
        Consumer<Long, String> consumer = createConsumer();
        try {
            System.out.print("Entrez une commande: ");
            commands();
            while (true) {
                ConsumerRecords<Long, String> consumerRecords = consumer.poll(Duration.ofMillis(1000));
                JSONParser parser = new JSONParser();

                consumerRecords.forEach(record -> {
                    String value = record.value();
                    String key = String.valueOf(record.key());
                    switch (key) {
                        case "get_global_values":
                            try {
                                Object obj = parser.parse(value);
                                JSONObject jsonObject = (JSONObject) obj;
                                System.out.println("Nombre de cas confirmés total: " + jsonObject.get("TotalConfirmed"));
                                System.out.println("Nombre de nouveaux décès confirmés: " + jsonObject.get("NewDeaths"));
                                System.out.println("Nombre de décès total: " + jsonObject.get("TotalDeaths"));
                                System.out.println("Nombre de nouveaux rétablissement: " + jsonObject.get("NewRecovered"));
                                System.out.println("Nombre total de personnes rétablies: " + jsonObject.get("TotalRecovered"));
                            } catch (ParseException e) {
                                e.printStackTrace();
                            }
                            break;
                        case "get_confirmed_avg":
                            System.out.println("Moyenne de personne confirmés: " + value);
                            break;
                        case "get_deaths_avg":
                            System.out.println("Moyenne des décès: " + value);
                            break;
                        case "get_countries_deaths_percent":
                            System.out.println("Pourcentage de décès par rapport aux cas confirmés: " + value + "%");
                            break;
                        case "get_country_values":
                            try {
                                Object obj = parser.parse(value);
                                JSONObject jsonObject = (JSONObject) obj;
                                System.out.println("Pays: " + jsonObject.get("Country"));
                                System.out.println("Nouveaux cas confirmés: " + jsonObject.get("NewConfirmed"));
                                System.out.println("Nombre de cas confirmés: " + jsonObject.get("TotalConfirmed"));
                                System.out.println("Nombre de nouveaux décès confirmés: " + jsonObject.get("NewDeaths"));
                                System.out.println("Nombre total de décès: " + jsonObject.get("TotalDeaths"));
                                System.out.println("Nombre total de personnes rétablies: " + jsonObject.get("TotalRecovered"));
                                System.out.println("En date du : " + jsonObject.get("DateMaj"));
                            } catch (ParseException e) {
                                e.printStackTrace();
                            }
                            break;
                        default:
                            System.out.println("Réponse inconnue");
                            break;
                    }
                });
                consumer.commitAsync();
                System.out.print("Entrez une commande: ");
                commands();
            }
        } finally {
            consumer.close();
        }
    }

    public Producer<String, String> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKER);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "Pr2");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return new KafkaProducer<>(props);
    }

    public Consumer<Long, String> createConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKER);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_ID_CONFIG);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, MAX_POLL_RECORDS);

        Consumer<Long, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(TOPIC_3));
        return consumer;
    }

    public void commands() throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        String inputUser = reader.readLine();

        Producer<String, String> pr2 = createProducer();
        ProducerRecord<String, String> producerRecord;

        switch (inputUser) {
            case "help":
                System.out.println("get_global_values (retourne les valeurs globales Global du fichier json)");
                System.out.println("get_country_values v_pays (retourne les valeurs du pays demandé ou v_pays est " +
                        "une chaine de caractères du pays demandé)");
                System.out.println("get_confirmed_avg (retourne une moyenne des cas confirmés sum(pays)/nb(pays))");
                System.out.println("get_deaths_avg (retourne une moyenne des Décès sum(pays)/nb(pays))");
                System.out.println("get_countries_deaths_percent (retourne le pourcentage de Décès par rapport aux " +
                        "cas confirmés)");
                System.out.println("help (Affiche la liste des commandes et une explication comme ci-dessus)");
                break;
            case "get_global_values":
            case "get_confirmed_avg":
            case "get_countries_deaths_percent":
            case "get_deaths_avg":
                producerRecord = new ProducerRecord<>(TOPIC_2, "data", inputUser);
                pr2.send(producerRecord);
                System.out.println("Envoi de la commande sur le Topic2");
                System.out.println("En attente de la réponse ...");
                break;
            default:
                /* On dérive un peu le switch pour gérer un param supplémentaire au cas ou on tombe dans le cas du
                    get_country_values v_pays ou v_pays est un paramètre que l'utilisateur entrera */
                try {
                    String[] param = inputUser.split(" ");
                    if (param[0].equals("get_country_values") && param.length == 2) {
                        producerRecord = new ProducerRecord<>(TOPIC_2, "data", inputUser);
                        pr2.send(producerRecord);
                    } else {
                        System.out.println("Commande inconnue, help pour avoir la liste des commandes disponibles");
                    }
                } catch (ArrayIndexOutOfBoundsException e) {
                    System.out.println("Il manque le paramètre v_pays");
                }
                break;
        }
        pr2.close();
    }
}
