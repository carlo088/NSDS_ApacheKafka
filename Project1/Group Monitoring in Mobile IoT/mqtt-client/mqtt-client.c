/* COOJA mote
 * this mote represents the device of each person.
 */

/* Libraries definition */
#include "contiki.h"
#include "string.h"
#include "random.h"
#include "stdlib.h"

#include "mqtt.h"
#include "rpl.h"
#include "net/ipv6/uip.h"
#include "net/ipv6/sicslowpan.h"
#include "sys/etimer.h"
#include "sys/ctimer.h"

#include "net/routing/routing.h"
#include "net/netstack.h"
#include "net/link-stats.h"
#include "net/ipv6/simple-udp.h"

#include "sys/log.h"
/*---------------------------------------------------------------------------*/
/* Log Configuration */
#define LOG_MODULE "MQTT-MOTE"
#define LOG_LEVEL LOG_LEVEL_INFO
/*---------------------------------------------------------------------------*/
/* MQTT PROCESS parameters */
#define MQTT_BROKER_IP_ADDR         "fd00::1"
#define MQTT_REGISTER_TOPIC         "nsds2023/newEnvironmentPerson"
#define MQTT_CREATE_TOPIC           "nsds2023/createGroup"
#define MQTT_CHANGE_TOPIC           "nsds2023/changeCardinality"
/*---------------------------------------------------------------------------*/
/* Publish to a local MQTT broker (e.g. mosquitto) running on the node that hosts your border router */
static const char *broker_ip = MQTT_BROKER_IP_ADDR;
#define DEFAULT_ORG_ID              "mqtt-mote"
/*---------------------------------------------------------------------------*/
/* A timeout used when waiting for something to happen (e.g. to connect or to disconnect) */
#define STATE_MACHINE_PERIODIC     (CLOCK_SECOND >> 1)
/*---------------------------------------------------------------------------*/
/* Connections and reconnections */
#define RETRY_FOREVER              0xFF
#define RECONNECT_INTERVAL         (CLOCK_SECOND * 2)
/*---------------------------------------------------------------------------*/
/* Number of times to try reconnecting to the broker. Can be a limited number (e.g. 3, 10 etc) or can be set to RETRY_FOREVER */
#define RECONNECT_ATTEMPTS         RETRY_FOREVER
#define CONNECTION_STABLE_TIME     (CLOCK_SECOND * 5)
static struct timer connection_life;
static uint8_t connect_attempt;
/*---------------------------------------------------------------------------*/
/* States of the MQTT client */
static uint8_t state;
#define STATE_INIT            0
#define STATE_REGISTERED      1
#define STATE_CONNECTING      2
#define STATE_CONNECTED       3
#define STATE_PUBLISHING      4
#define STATE_DISCONNECTED    5
#define STATE_CONFIG_ERROR 0xFE
#define STATE_ERROR        0xFF
/*---------------------------------------------------------------------------*/
/* Config client */
#define CONFIG_ORG_ID_LEN        32
#define CONFIG_TYPE_ID_LEN       32
#define CONFIG_AUTH_TOKEN_LEN    32
#define CONFIG_CMD_TYPE_LEN       8
#define CONFIG_IP_ADDR_STR_LEN   64
/*---------------------------------------------------------------------------*/
/* A timeout used when waiting to connect to a network */
#define NET_CONNECT_PERIODIC        (CLOCK_SECOND >> 2)
/*---------------------------------------------------------------------------*/
/* Default configuration values */
#define DEFAULT_TYPE_ID             "native"
#define DEFAULT_AUTH_TOKEN          "AUTHZ"
#define DEFAULT_SUBSCRIBE_CMD_TYPE  "+"
#define DEFAULT_BROKER_PORT         1883
#define DEFAULT_PUBLISH_INTERVAL    (60 * CLOCK_SECOND)
#define DEFAULT_KEEP_ALIVE_TIMER    60
/*---------------------------------------------------------------------------*/
/* Maximum TCP segment size for outgoing segments of our socket */
#define MAX_TCP_SEGMENT_SIZE    32
/*---------------------------------------------------------------------------*/
/* Define the size of the buffers. Make sure they are large enough to hold the entire respective string. We also need space for the null termination */
#define BUFFER_SIZE 64
#define ADDRESS_SIZE 32
static char client_id[BUFFER_SIZE];
static char register_topic[BUFFER_SIZE];
static char create_topic[BUFFER_SIZE];
static char change_topic[BUFFER_SIZE];
/*---------------------------------------------------------------------------*/
/* The main MQTT buffer, used to store the strings published on a topic. We will need to increase if we start publishing more data. */
#define APP_BUFFER_SIZE 512
static char app_buffer[APP_BUFFER_SIZE];
/*---------------------------------------------------------------------------*/
/* Data structure declaration for the MQTT client configuration */
typedef struct mqtt_client_config {
  char org_id[CONFIG_ORG_ID_LEN];
  char type_id[CONFIG_TYPE_ID_LEN];
  char auth_token[CONFIG_AUTH_TOKEN_LEN];
  char broker_ip[CONFIG_IP_ADDR_STR_LEN];
  char cmd_type[CONFIG_CMD_TYPE_LEN];
  clock_time_t pub_interval;
  uint16_t broker_port;
} mqtt_client_config_t;

static mqtt_client_config_t conf;
/*---------------------------------------------------------------------------*/
/* MQTT PROCESS global variables */
static struct mqtt_connection conn;
static struct mqtt_message *msg_ptr = 0;
static struct etimer publish_periodic_timer;
static char *buf_ptr;
static bool connected_to_broker = false;
/*---------------------------------------------------------------------------*/
/* Data structure declaration for list of IP address */
typedef struct list_node_t {
    char ipaddr[ADDRESS_SIZE];
    struct list_node_t *next;
} list_node_t ;
typedef list_node_t* list_ipaddr_t;
/*---------------------------------------------------------------------------*/
/* Group PROCESS global variables */
#define PERIODIC_INTERVAL (CLOCK_SECOND * 10)
#define NATIONALITY_SIZE 20
#define MAX_AGE 80
#define MIN_AGE 10
#define NAT_SET_SIZE 10
#define UDP_PORT 8765

static struct simple_udp_connection udp_conn;
static int age;
static char nationality[NATIONALITY_SIZE];
static uip_ipaddr_t leader_ipaddr;
static list_ipaddr_t group = NULL;
static int group_cardinality;
/*---------------------------------------------------------------------------*/
/* States of the mote as member of a group */
static uint8_t group_state;
#define STATE_REGISTER        0
#define STATE_SEARCHING       1
#define STATE_LEADER          2
/*---------------------------------------------------------------------------*/
/* This function adds an address to the list */
static list_ipaddr_t add_to_list(char* addr, list_ipaddr_t l) {
    list_ipaddr_t new_node;
    new_node = (list_ipaddr_t)malloc(sizeof(list_node_t));
    if (new_node) {
        strcpy(new_node->ipaddr, addr);
        new_node->next = l;
    } else
        LOG_ERR("Memory allocation error\n");
    return new_node;
}
/*---------------------------------------------------------------------------*/
/* This function returns true if the list contains the address */
static bool contains(list_ipaddr_t l, char* addr) {   
    list_ipaddr_t temp;
    temp = l;
    while (temp != NULL && strcmp(addr, temp->ipaddr) != 0) {
        temp = temp->next;
    }
    if (temp == NULL)
        return false;
    else
        return true;
}
/*---------------------------------------------------------------------------*/
/* This function removes an address from the list */
static void deleteElement(list_ipaddr_t l, char* addr) { 
    list_ipaddr_t temp = l;
    list_ipaddr_t prev; 
  
    if (temp != NULL && strcmp(temp->ipaddr, addr) == 0) { 
        l = temp->next; 
        free(temp); 
        return; 
    } 
  
    while (temp != NULL && strcmp(temp->ipaddr, addr) != 0) { 
        prev = temp;
        temp = temp->next;
    } 
  
    if (temp == NULL) 
        return; 
  
    prev->next = temp->next; 
    free(temp);
}
/*---------------------------------------------------------------------------*/
/* This function returns a string containing the IP address (in the form 20x:x:x:x) */
static char addr_buffer[ADDRESS_SIZE];
static char* get_ipaddr(const uip_ipaddr_t *ipaddr) {
    char *addr_ptr = addr_buffer;
    uiplib_ipaddr_snprint(addr_buffer, ADDRESS_SIZE, ipaddr);
    // The type uip_ipaddr_t is fd00::20x:x:x:x   --> we have to trim it
    addr_ptr += 6;
    return addr_ptr;
}
/*---------------------------------------------------------------------------*/
/* Processes definition */
PROCESS(mqtt_client_process, "MQTT Client PROCESS");
PROCESS(group_process, "Group PROCESS");
AUTOSTART_PROCESSES(&mqtt_client_process, &group_process);
/*---------------------------------------------------------------------------*/
/* Publish on a specific topic */
static void publish(char* topic)
{
    static char pub_topic[BUFFER_SIZE];

    // Publish on nsds2023/newEnvironmentPerson
    if(strcmp(topic, MQTT_REGISTER_TOPIC) == 0) {
      strcpy(pub_topic, register_topic);

      int len;
      int remaining = APP_BUFFER_SIZE;
      buf_ptr = app_buffer;

      // Write the payload
      len = snprintf(buf_ptr, remaining, "{\"IP\":\"%s\",\"nationality\":\"%s\",\"age\":%d}", get_ipaddr(rpl_get_global_address()), nationality, age);
      if(len < 0 || len >= remaining) {
          LOG_ERR("Buffer too short. Have %d, need %d + \\0\n", remaining, len);
          return;
      }
      remaining -= len;
      buf_ptr += len;
    }

    // Publish on nsds2023/createGroup or nsds2023/changeCardinality
    if(strcmp(topic, MQTT_CREATE_TOPIC) == 0 || strcmp(topic, MQTT_CHANGE_TOPIC) == 0) {
      strcpy(pub_topic, topic);

      int len;
      int remaining = APP_BUFFER_SIZE;
      buf_ptr = app_buffer;

      // Write the payload
      len = snprintf(buf_ptr, remaining, "{\"leader\":\"%s\",\"members\":[", get_ipaddr(&(leader_ipaddr)));
      if(len < 0 || len >= remaining) {
          LOG_ERR("Buffer too short. Have %d, need %d + \\0\n", remaining, len);
          return;
      }
      remaining -= len;
      buf_ptr += len;
      
      list_ipaddr_t temp = group;
      group_cardinality = 0;
      while (temp != NULL) {
        group_cardinality++;

        if (group_cardinality == 1)
          len = snprintf(buf_ptr, remaining, "\"%s\"", temp->ipaddr);
        else
          len = snprintf(buf_ptr, remaining, ",\"%s\"", temp->ipaddr);

        if(len < 0 || len >= remaining) {
            LOG_ERR("Buffer too short. Have %d, need %d + \\0\n", remaining, len);
            return;
        }
        remaining -= len;
        buf_ptr += len;

        temp = temp->next;
      }

      len = snprintf(buf_ptr, remaining, "],\"cardinality\":%d}", group_cardinality);
      if(len < 0 || len >= remaining) {
          LOG_ERR("Buffer too short. Have %d, need %d + \\0\n", remaining, len);
          return;
      }
      remaining -= len;
      buf_ptr += len;
    }

    mqtt_publish(&conn, NULL, pub_topic, (uint8_t *)app_buffer, strlen(app_buffer), MQTT_QOS_LEVEL_1, MQTT_RETAIN_OFF);
    LOG_INFO("Publishing on topic %s...\n", pub_topic);
}
/*---------------------------------------------------------------------------*/
/* This function is triggered whenever an event occurs on the MQTT device */
static void mqtt_event(struct mqtt_connection *m, mqtt_event_t event, void *data)
{
  switch(event) {
  case MQTT_EVENT_CONNECTED: {
    LOG_INFO("Application has a MQTT connection!\n");
    timer_set(&connection_life, CONNECTION_STABLE_TIME);
    state = STATE_CONNECTED;
    break;
  }
  case MQTT_EVENT_DISCONNECTED: {
    LOG_INFO("MQTT Disconnect: reason %u\n", *((mqtt_event_t *)data));

    state = STATE_DISCONNECTED;
    process_poll(&mqtt_client_process);
    break;
  }
  case MQTT_EVENT_PUBLISH: {
    msg_ptr = data;

    if(msg_ptr->first_chunk) {
      msg_ptr->first_chunk = 0;
      LOG_INFO("Application received a publish on topic '%s'\n", msg_ptr->topic);
    }
    break;
  }
  case MQTT_EVENT_SUBACK: {
    LOG_INFO("Application is subscribed to topic successfully\n");
    break;
  }
  case MQTT_EVENT_UNSUBACK: {
    LOG_INFO("Application is unsubscribed to topic successfully\n");
    break;
  }
  case MQTT_EVENT_PUBACK: {
    LOG_INFO("Publishing complete\n");
    break;
  }
  default:
    LOG_WARN("Application got a unhandled MQTT event: %i\n", event);
    break;
  }
}
/*---------------------------------------------------------------------------*/
/* Creates the main topics of the mote */
static int construct_topics(void)
{
    // Create the topic: nsds2023/newEnvironmentPerson
    buf_ptr = register_topic;

    int len = snprintf(buf_ptr, BUFFER_SIZE, MQTT_REGISTER_TOPIC);
    if(len < 0 || len >= BUFFER_SIZE) {
        LOG_ERR("Topic: %d, buffer %d\n", len, BUFFER_SIZE);
        return 0;
    }
    
    // Create the topic: nsds2023/createGroups
    buf_ptr = create_topic;

    len = snprintf(buf_ptr, BUFFER_SIZE, MQTT_CREATE_TOPIC);
    if(len < 0 || len >= BUFFER_SIZE) {
        LOG_ERR("Topic: %d, buffer %d\n", len, BUFFER_SIZE);
        return 0;
    }

    // Create the topic: nsds2023/changeCardinality
    buf_ptr = change_topic;

    len = snprintf(buf_ptr, BUFFER_SIZE, MQTT_CHANGE_TOPIC);
    if(len < 0 || len >= BUFFER_SIZE) {
        LOG_ERR("Topic: %d, buffer %d\n", len, BUFFER_SIZE);
        return 0;
    }

    return 1;
}
/*---------------------------------------------------------------------------*/
/* Create the configuration of the client ID for MQTT communication */
static int construct_client_id(void)
{
  int len = snprintf(client_id, BUFFER_SIZE, "d:%s:%s:%02x%02x%02x%02x%02x%02x",
                     conf.org_id, conf.type_id,
                     linkaddr_node_addr.u8[0], linkaddr_node_addr.u8[1],
                     linkaddr_node_addr.u8[2], linkaddr_node_addr.u8[5],
                     linkaddr_node_addr.u8[6], linkaddr_node_addr.u8[7]);

  /* len < 0: Error. Len >= BUFFER_SIZE: Buffer too small */
  if(len < 0 || len >= BUFFER_SIZE) {
    LOG_INFO("Client ID: %d, Buffer %d\n", len, BUFFER_SIZE);
    return 0;
  }

  return 1;
}
/*---------------------------------------------------------------------------*/
/* Updates the configuration -> client_id, topics */
static void update_config(void)
{
  if(construct_client_id() == 0) {
    /* Fatal error. Client ID larger than the buffer */
    state = STATE_CONFIG_ERROR;
    return;
  }

  if(construct_topics() == 0) {
    /* Fatal error. Topic larger than the buffer */
    state = STATE_CONFIG_ERROR;
    return;
  }

  state = STATE_INIT;

  /*
   * Schedule next timer event ASAP
   *
   * If we entered an error state then we won't do anything when it fires
   *
   * Since the error at this stage is a config error, we will only exit this
   * error state if we get a new config
   */
  etimer_set(&publish_periodic_timer, 0);

  return;
}
/*---------------------------------------------------------------------------*/
/* Initialize the configuration parameters */
static void init_config()
{
  /* Populate configuration with default values */
  memset(&conf, 0, sizeof(mqtt_client_config_t));

  memcpy(conf.org_id, DEFAULT_ORG_ID, strlen(DEFAULT_ORG_ID));
  memcpy(conf.type_id, DEFAULT_TYPE_ID, strlen(DEFAULT_TYPE_ID));
  memcpy(conf.auth_token, DEFAULT_AUTH_TOKEN, strlen(DEFAULT_AUTH_TOKEN));
  memcpy(conf.broker_ip, broker_ip, strlen(broker_ip));
  memcpy(conf.cmd_type, DEFAULT_SUBSCRIBE_CMD_TYPE, 1);

  conf.broker_port = DEFAULT_BROKER_PORT;
  conf.pub_interval = DEFAULT_PUBLISH_INTERVAL;
}
/*---------------------------------------------------------------------------*/
/* Connects the MQTT mote to the broker (mosquitto) */
static void connect_to_broker(void)
{
  /* Connect to MQTT server */
  mqtt_connect(&conn, conf.broker_ip, conf.broker_port, conf.pub_interval * 3);
  state = STATE_CONNECTING;
}
/*---------------------------------------------------------------------------*/
/* Function to change the state of the machine on the MQTT mote */
static void state_machine(void)
{
  switch(state) {
    case STATE_INIT:
      LOG_INFO("STATE INIT\n");
      /* If we have just been configured register MQTT connection */
      mqtt_register(&conn, &mqtt_client_process, client_id, mqtt_event, MAX_TCP_SEGMENT_SIZE);
      mqtt_set_username_password(&conn, "use-token-auth", conf.auth_token);

      /* _register() will set auto_reconnect; we don't want that */
      conn.auto_reconnect = 0;
      connect_attempt = 1;

      state = STATE_REGISTERED;
      /* Continue (don't break the switch case) */
    case STATE_REGISTERED:
      if(uip_ds6_get_global(ADDR_PREFERRED) != NULL) {
        /* Registered and with a global IPv6 address, connect! */
        LOG_INFO("Joined network! Connect attempt %u\n", connect_attempt);
        connect_to_broker();
        LOG_INFO("Connecting: retry %u...\n", connect_attempt);
      }
      etimer_set(&publish_periodic_timer, NET_CONNECT_PERIODIC);
      return;
      break;
    case STATE_CONNECTING:
      /* Not connected yet. Wait */
      break;
    case STATE_CONNECTED:
    case STATE_PUBLISHING:
      /* If the timer expired, the connection is stable, no more attempts is needed */
      if(timer_expired(&connection_life)) {
        /* Intentionally using 0 here instead of 1: We want RECONNECT_ATTEMPTS attempts if we disconnect after a successful connect */
        connect_attempt = 0;
      }

      if(mqtt_ready(&conn) && conn.out_buffer_sent) {
        /* Connected; publish */
        connected_to_broker = true;

        if(state == STATE_CONNECTED) {
          LOG_INFO("Ready to publish!\n");
          state = STATE_PUBLISHING;
        }
      } else {
        connected_to_broker = false;
      }
      break;
    case STATE_DISCONNECTED:
      LOG_INFO("Disconnected\n");
      if(connect_attempt < RECONNECT_ATTEMPTS || RECONNECT_ATTEMPTS == RETRY_FOREVER) {
        /* Disconnect and backoff */
        clock_time_t interval;
        mqtt_disconnect(&conn);
        connect_attempt++;

        interval = connect_attempt < 3 ? RECONNECT_INTERVAL << connect_attempt : RECONNECT_INTERVAL << 3;

        LOG_INFO("Disconnected: attempt %u in %lu ticks\n", connect_attempt, interval);
        etimer_set(&publish_periodic_timer, interval);

        state = STATE_REGISTERED;
        return;
      } else {
        /* Max reconnect attempts reached; enter error state */
        state = STATE_ERROR;
        LOG_ERR("Aborting connection after %u attempts\n", connect_attempt - 1);
      }
      break;
    case STATE_CONFIG_ERROR:
      /* Idle away. The only way out is a new config */
      LOG_ERR("Bad configuration.\n");
      return;
    case STATE_ERROR:
    default:
      /*
      * 'default' should never happen
      *
      * If we enter here it's because of some error. Stop timers. The only thing
      * that can bring us out is a new config event
      */
      LOG_INFO("Default case: State=0x%02x\n", state);
      return;
  }

  /* If we didn't return so far, reschedule ourselves */
  etimer_set(&publish_periodic_timer, STATE_MACHINE_PERIODIC);
}
/*---------------------------------------------------------------------------*/
/* MQTT PROCESS */
PROCESS_THREAD(mqtt_client_process, ev, data){
  PROCESS_BEGIN();
    
  LOG_INFO("MQTT Mote Process\n");
  init_config();
  update_config();

  /* Main loop */

  while(1) {
    PROCESS_YIELD();

    if (ev == PROCESS_EVENT_TIMER && data == &publish_periodic_timer) {
      state_machine();
    }
  }
  PROCESS_END();
}
/*---------------------------------------------------------------------------*/
/* This function is called when receiving a UDP message */
static void udp_rx_callback(struct simple_udp_connection *c,
         const uip_ipaddr_t *sender_addr,
         uint16_t sender_port,
         const uip_ipaddr_t *receiver_addr,
         uint16_t receiver_port,
         const uint8_t *data,
         uint16_t datalen) {
      
        // Manage a list of contacts received by a neighbor
        list_ipaddr_t contacts = NULL;
        static uip_ds6_nbr_t *nbr;

        // Create a list with the contacts address of the sender
        char* addr = strtok((char*) data, " ");
        while (addr != NULL) {
          contacts = add_to_list(addr, contacts);
          addr = strtok(NULL, " ");
        }

        // If I'm still searching for a group
        if(group_state == STATE_SEARCHING) {
          list_ipaddr_t common = NULL;

          // RPL root
          uip_ipaddr_t root;
          NETSTACK_ROUTING.get_root_ipaddr(&root);
          static char root_ipaddr[ADDRESS_SIZE];
          strcpy(root_ipaddr, get_ipaddr(&root));

          // For each of my neighbors, see if it's in contact with the sender (find common contacts)
          for(nbr = nbr_table_head(ds6_neighbors); nbr != NULL; nbr = nbr_table_next(ds6_neighbors, nbr)) {
            if (contains(contacts, get_ipaddr(&(nbr->ipaddr))) && strcmp(get_ipaddr(&(nbr->ipaddr)), root_ipaddr) != 0) {    // exclude RPL root
              common = add_to_list(get_ipaddr(&(nbr->ipaddr)), common);
            }
          }

          // If there is at least one common contact, a group is formed (me, sender and the common contacts)
          if (common != NULL) {
            group = common;

            // Add me and the sender to the group
            group = add_to_list(get_ipaddr(sender_addr), group);
            group = add_to_list(get_ipaddr(rpl_get_global_address()), group);

            // Find the leader of the group (highest IP)
            static char leader[ADDRESS_SIZE];
            int max = 0;
            list_ipaddr_t temp;
            temp = group;
            while (temp != NULL) {
                static char addr[ADDRESS_SIZE];
                strcpy(addr, temp->ipaddr);
                int id = (int)strtol(strtok(addr, ":"), NULL, 16);
                if (id > max) {
                  max = id;
                  strcpy(leader, temp->ipaddr);
                }
                temp = temp->next;
            }

            // If I'm the leader, publish on nsds2023/createGroup and notify all the members
            if(strcmp(leader, get_ipaddr(rpl_get_global_address())) == 0) {
              // Set the leader and stop searching
              leader_ipaddr = *rpl_get_global_address();
              LOG_INFO("Creating the group...\n");
              publish(MQTT_CREATE_TOPIC);
              group_state = STATE_LEADER;
            }
            else {
              // I'm not the leader, wait for creation message sent by the leader
            }
          }
          return;
        }
        
        // If I'm the leader of my group, check if the sender can join my group
        if(group_state == STATE_LEADER){
          
          // To join my group the sender has to be in contact with at least all the members of the group
          list_ipaddr_t temp;
          temp = group;
          while (temp != NULL) {
            if(!contains(contacts, temp->ipaddr))
              return;
            temp = temp->next;
          }

          // Add member to my group
          group = add_to_list(get_ipaddr(sender_addr), group);
          publish(MQTT_CHANGE_TOPIC);
          return;
        }
        
        return;
}
/*---------------------------------------------------------------------------*/
/* Group PROCESS */
PROCESS_THREAD(group_process, ev, data){
    static struct etimer periodic_timer;
    static uip_ds6_nbr_t *nbr;

    PROCESS_BEGIN();
    
    // Extract a random age in the range
    age = (random_rand() % (MAX_AGE - MIN_AGE)) + MIN_AGE;

    // Extract a random nationality from a set
    char* nationalities[NAT_SET_SIZE] = {"Italian", "Spanish", "French", "German", "American", "Turkish", "Japanese", "Chinese", "Irish", "English"};
    strcpy(nationality, nationalities[random_rand() % NAT_SET_SIZE]);

    simple_udp_register(&udp_conn, UDP_PORT, NULL, UDP_PORT, udp_rx_callback);
    group_state = STATE_REGISTER;

    etimer_set(&periodic_timer, random_rand() % PERIODIC_INTERVAL);

    while(1) {
        // Wait until periodic timer expires
        PROCESS_WAIT_EVENT_UNTIL(etimer_expired(&periodic_timer));

        // If MQTT client is connected to MQTT broker and ROOT is reachable
        if (connected_to_broker && NETSTACK_ROUTING.node_is_reachable()){
          
          // Initially I must send nationality and age to the back-end
          if (group_state == STATE_REGISTER) {
            LOG_INFO("Register to back-end...\n");
            publish(MQTT_REGISTER_TOPIC);
            group_state = STATE_SEARCHING;
          }

          // If I'm searching for a group, send the list of my contacts to my neighbors
          else if (group_state == STATE_SEARCHING) {
            int len;
            int remaining = APP_BUFFER_SIZE;
            static char neighbors[APP_BUFFER_SIZE];
            buf_ptr = neighbors;

            // Write in the buffer a list of my contacts' IP address (separated by comma)
            for(nbr = nbr_table_head(ds6_neighbors); nbr != NULL; nbr = nbr_table_next(ds6_neighbors, nbr)) {
              char *addr = get_ipaddr(&(nbr->ipaddr));

              len = snprintf(buf_ptr, remaining, "%s ", addr);
              if(len < 0 || len >= remaining) {
                  LOG_ERR("Buffer too short. Have %d, need %d + \\0\n", remaining, len);
              }

              remaining -= len;
              buf_ptr += len;
            }

            // Send to all my neighbors the list of my contacts through UDP
            for(nbr = nbr_table_head(ds6_neighbors); nbr != NULL; nbr = nbr_table_next(ds6_neighbors, nbr)) {
              
              // To send to multiple hosts we need to introduce a small delay
              etimer_set(&periodic_timer, CLOCK_SECOND);
              PROCESS_WAIT_EVENT_UNTIL(etimer_expired(&periodic_timer));

              // Send a different pointer to avoid allocation corruption
              char message[APP_BUFFER_SIZE];
              strcpy(message, neighbors);
              simple_udp_sendto(&udp_conn, &message, strlen(message) + 1, &(nbr->ipaddr));
            }
          }

          // If I'm the leader, check if members are still in contact with me
          else if (group_state == STATE_LEADER) {
            list_ipaddr_t temp;
            temp = group;
            while (temp != NULL) {
              bool found = false;
              for(nbr = nbr_table_head(ds6_neighbors); nbr != NULL; nbr = nbr_table_next(ds6_neighbors, nbr)) {
                if(strcmp(temp->ipaddr, get_ipaddr(&(nbr->ipaddr))) == 0) {
                  found = true;
                  break;
                }
              }
              if (!found && strcmp(temp->ipaddr, get_ipaddr(rpl_get_global_address())) != 0) {
                // Remove from the group
                LOG_INFO("Member %s left the group\n", temp->ipaddr);
                deleteElement(group, temp->ipaddr);
                publish(MQTT_CHANGE_TOPIC);
              }
              temp = temp->next;
            }
        
            if (group_cardinality < 3) {
              LOG_INFO("Deleting group...\n");
              group = NULL;
              group_state = STATE_SEARCHING;
            }
          }

        }

        // Refresh neighbor table
        for(rpl_nbr_t *nbr = nbr_table_head(rpl_neighbors); nbr != NULL; nbr = nbr_table_next(rpl_neighbors, nbr)) {
          const struct link_stats *stats = rpl_neighbor_get_link_stats(nbr);
          clock_time_t clock_now = clock_time();
          if (stats != NULL && stats->last_tx_time > 0) {
            unsigned elapsed_time = (unsigned) ((clock_now - stats->last_tx_time) / (60 * CLOCK_SECOND));
            if (elapsed_time > 2)
              uip_ds6_nbr_rm(uip_ds6_nbr_lookup(rpl_neighbor_get_ipaddr(nbr)));
          }
        }

        // Add some jitter (+- 5 seconds)
        etimer_set(&periodic_timer, PERIODIC_INTERVAL - (5 *CLOCK_SECOND) + (random_rand() % (10 * CLOCK_SECOND)));
    }
    PROCESS_END();
}
