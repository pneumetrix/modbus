/*
 * Copyright 2021 Suyash Mathema
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

#include "mgos_modbus.h"

#include "crc16.h"
#include "mgos_rpc.h"

#define bitRead(value, bit) (((value) >> (bit)) & 0x01)
#define lowByte(w) ((uint8_t)((w)&0xff))
#define highByte(w) ((uint8_t)((w) >> 8))

enum uart_read_states { DISABLED,
                        READ_START,
                        RESP_METADATA,
                        RESP_COMPLETE };

struct mgos_modbus {
    struct mbuf receive_buffer;
    struct mbuf transmit_buffer;
    mb_response_callback cb;
    void* cb_arg;
    int uart_no;
    uint8_t slave_id_u8;
    uint16_t read_address_u16;
    uint16_t read_qty_u16;
    uint16_t write_address_u16;
    uint16_t write_qty_u16;
    uint8_t* write_data;
    uint8_t write_data_len;
    uint8_t mask_and;
    uint8_t mask_or;
    uint8_t func_code_u8;
    uint8_t resp_status_u8;
    uint8_t resp_bytes_u8;
    enum uart_read_states read_state;
};

static struct mgos_modbus* s_modbus = NULL;
static mgos_timer_id req_timer;

static void print_buffer(struct mbuf buffer) {
    char str[1024];
    int length = 0;
    for (int i = 0; i < buffer.len && i < sizeof(str) / 3; i++) {
        length += sprintf(str + length, "%.2x ", buffer.buf[i]);
    }
    LOG(LL_DEBUG, ("SlaveID: %.2x, Function: %.2x - Buffer: %.*s", s_modbus->slave_id_u8, s_modbus->func_code_u8, length, str));
}

static size_t mbuf_append_16(struct mbuf* buffer, uint16_t value) {
    size_t size = 0;
    uint8_t lb = lowByte(value);
    uint8_t hb = highByte(value);
    size = mbuf_append(buffer, &hb, sizeof(uint8_t));
    size += mbuf_append(buffer, &lb, sizeof(uint8_t));
    return size;
}

static uint16_t calculate_crc16(struct mbuf value) {
    uint16_t u16CRC = 0xFFFF;
    for (int i = 0; i < value.len; i++) {
        u16CRC = crc16_update(u16CRC, (uint8_t)value.buf[i]);
    }

    return (lowByte(u16CRC) << 8) | (highByte(u16CRC) & 0xff);  //CRC Lower Byte first then Higher Byte
}

static uint8_t verify_crc16(struct mbuf value) {
    value.len -= 2;
    uint16_t u16CRC = calculate_crc16(value);
    if (highByte(u16CRC) != value.buf[value.len] || lowByte(u16CRC) != value.buf[value.len + 1]) {  //Since calculate returns lower byte first
        return RESP_INVALID_CRC;
    }
    return RESP_SUCCESS;
}

/*
Callback function that is called when a set timeout period expires before receiving a response.
*/
static void req_timeout_cb(void* arg) {
    LOG(LL_DEBUG, ("SlaveID: %.2x, Function: %.2x - Request timed out", s_modbus->slave_id_u8, s_modbus->func_code_u8));
    s_modbus->resp_status_u8 = RESP_TIMED_OUT;
    struct mb_request_info ri = {
        s_modbus->slave_id_u8,
        s_modbus->read_address_u16,
        s_modbus->read_qty_u16,
        s_modbus->write_address_u16,
        s_modbus->write_qty_u16,
        s_modbus->mask_and,
        s_modbus->mask_or,
        s_modbus->func_code_u8,
    };
    s_modbus->cb(RESP_TIMED_OUT, ri, s_modbus->receive_buffer, s_modbus->cb_arg);
    s_modbus->read_state = DISABLED;
    req_timer = 0;
    (void)arg;
}

/*
To validate slave id, function code and modbus exception in the response and
estimate size of the requested data from the response.
*/
static bool validate_mb_metadata(struct mbuf* buffer) {
    // verify response is for correct Modbus slave
    if ((uint8_t)buffer->buf[0] != s_modbus->slave_id_u8) {
        s_modbus->resp_status_u8 = RESP_INVALID_SLAVE_ID;
    }
    // verify response is for correct Modbus function code (mask exception bit 7)
    if (((uint8_t)buffer->buf[1] & 0x7F) != s_modbus->func_code_u8) {
        s_modbus->resp_status_u8 = RESP_INVALID_FUNCTION;
    }
    // check whether Modbus exception occurred; return Modbus Exception Code
    if (bitRead((uint8_t)buffer->buf[1], 7)) {
        s_modbus->resp_status_u8 = (uint8_t)buffer->buf[2];
    }

    if (s_modbus->resp_status_u8) {
        return false;
    }

    // evaluate returned Modbus function code to get modbus requested data size
    uint8_t resp_func = buffer->buf[1];
    if (resp_func == FUNC_READ_COILS ||
        resp_func == FUNC_READ_DISCRETE_INPUTS ||
        resp_func == FUNC_READ_INPUT_REGISTERS ||
        resp_func == FUNC_READ_HOLDING_REGISTERS ||
        resp_func == FUNC_READ_WRITE_MULTIPLE_REGISTERS) {
        s_modbus->resp_bytes_u8 = 5 + (uint8_t)buffer->buf[2];
    } else if (resp_func == FUNC_WRITE_SINGLE_COIL ||
               resp_func == FUNC_WRITE_MULTIPLE_COILS ||
               resp_func == FUNC_WRITE_SINGLE_REGISTER ||
               resp_func == FUNC_WRITE_MULTIPLE_REGISTERS) {
        s_modbus->resp_bytes_u8 = 8;
    } else if (resp_func == FUNC_MASK_WRITE_REGISTER) {
        s_modbus->resp_bytes_u8 = 10;
    }
    return true;
}

static void update_modbus_read_state(struct mbuf* buffer) {
    struct mb_request_info ri = {
        s_modbus->slave_id_u8,
        s_modbus->read_address_u16,
        s_modbus->read_qty_u16,
        s_modbus->write_address_u16,
        s_modbus->write_qty_u16,
        s_modbus->mask_and,
        s_modbus->mask_or,
        s_modbus->func_code_u8,
    };

    switch (s_modbus->read_state) {
        case DISABLED:
            /*
    Do not disable RX on default condition similar to RS485 control.
    Buffer piles up with grabage values once it is enabled. Just discard
    any values received while not expecting any response.
    */
            mbuf_clear(buffer);
            return;
        case READ_START:
            LOG(LL_VERBOSE_DEBUG, ("SlaveID: %.2x, Function: %.2x - Read modbus response start", s_modbus->slave_id_u8, s_modbus->func_code_u8));
            int count = 0;
            for (int i = 0; i < buffer->len; i++) {
                if (buffer->buf[i] != s_modbus->slave_id_u8) {
                    count++;
                } else {
                    mbuf_remove(buffer, count);
                    s_modbus->read_state = RESP_METADATA;
                    update_modbus_read_state(buffer);
                    return;
                }
            }
            mbuf_remove(buffer, count);
            return;
        case RESP_METADATA:
            if (buffer->len < s_modbus->resp_bytes_u8) {
                return;
            }
            if (!validate_mb_metadata(buffer)) {
                LOG(LL_DEBUG, ("SlaveID: %.2x, Function: %.2x - Invalid Response: %.2x, SlaveID: %.2x, Function: %.2x",
                               s_modbus->slave_id_u8, s_modbus->func_code_u8, s_modbus->resp_status_u8,
                               (uint8_t)buffer->buf[0], (uint8_t)buffer->buf[1]));
                break;
            }
            s_modbus->read_state = RESP_COMPLETE;
            update_modbus_read_state(buffer);
            return;
        case RESP_COMPLETE:
            if (buffer->len < s_modbus->resp_bytes_u8) {
                return;
            }
            buffer->len = s_modbus->resp_bytes_u8;
            if (verify_crc16(*buffer) == RESP_INVALID_CRC) {
                LOG(LL_DEBUG, ("SlaveID: %.2x, Function: %.2x - Invalid CRC", s_modbus->slave_id_u8, s_modbus->func_code_u8));
                s_modbus->resp_status_u8 = RESP_INVALID_CRC;
                break;
            }

            LOG(LL_DEBUG, ("SlaveID: %.2x, Function: %.2x - Modbus response received %d",
                           s_modbus->slave_id_u8, s_modbus->func_code_u8, s_modbus->receive_buffer.len));
            s_modbus->resp_status_u8 = RESP_SUCCESS;
            break;
        default:
            return;
    }
    print_buffer(s_modbus->receive_buffer);
    mgos_clear_timer(req_timer);
    s_modbus->read_state = DISABLED;
    s_modbus->cb(s_modbus->resp_status_u8, ri, s_modbus->receive_buffer, s_modbus->cb_arg);
}

static void uart_cb(int uart_no, void* param) {
    (void)param;
    assert(uart_no == s_modbus->uart_no);
    struct mbuf* buffer = &s_modbus->receive_buffer;

    size_t rx_av = mgos_uart_read_avail(uart_no);
    if (rx_av == 0) {
        return;
    }

    mgos_uart_read_mbuf(uart_no, buffer, rx_av);
    LOG(LL_VERBOSE_DEBUG, ("SlaveID: %.2x, Function: %.2x - uart_cb - Receive Buffer: %d, Read Available: %d",
                           s_modbus->slave_id_u8, s_modbus->func_code_u8, s_modbus->receive_buffer.len, rx_av));
    update_modbus_read_state(buffer);
}

static bool init_modbus(uint8_t slave_id, uint8_t func_code, uint8_t total_resp_bytes, mb_response_callback cb, void* cb_arg) {
    if (s_modbus->read_state != DISABLED)
        return false;
    s_modbus->cb = cb;
    s_modbus->cb_arg = cb_arg;
    s_modbus->resp_bytes_u8 = total_resp_bytes;
    s_modbus->slave_id_u8 = slave_id;
    s_modbus->func_code_u8 = func_code;
    LOG(LL_DEBUG, ("SlaveID: %.2x, Function: %.2x - Initialize Modbus", s_modbus->slave_id_u8, s_modbus->func_code_u8));
    return true;
}

static void set_read_address(uint16_t read_address, uint16_t read_qty) {
    s_modbus->read_address_u16 = read_address;
    s_modbus->read_qty_u16 = read_qty;
}

static void set_write_address(uint16_t write_address, uint16_t write_qty, uint8_t* data, uint8_t len) {
    s_modbus->write_address_u16 = write_address;
    s_modbus->write_qty_u16 = write_qty;
    if (data != NULL || len > 0) {
        s_modbus->write_data = data;
        s_modbus->write_data_len = len;
    }
}

static size_t set_transmit_buffer() {
    mbuf_clear(&s_modbus->transmit_buffer);

    size_t append = mbuf_append(&s_modbus->transmit_buffer, &s_modbus->slave_id_u8, sizeof(uint8_t));
    append += mbuf_append(&s_modbus->transmit_buffer, &s_modbus->func_code_u8, sizeof(uint8_t));

    uint8_t func_code = s_modbus->func_code_u8;
    if (func_code == FUNC_READ_COILS ||
        func_code == FUNC_READ_DISCRETE_INPUTS ||
        func_code == FUNC_READ_INPUT_REGISTERS ||
        func_code == FUNC_READ_HOLDING_REGISTERS ||
        func_code == FUNC_READ_WRITE_MULTIPLE_REGISTERS) {
        append += mbuf_append_16(&s_modbus->transmit_buffer, s_modbus->read_address_u16);
        append += mbuf_append_16(&s_modbus->transmit_buffer, s_modbus->read_qty_u16);
    }

    if (func_code == FUNC_WRITE_SINGLE_COIL ||
        func_code == FUNC_WRITE_MULTIPLE_COILS ||
        func_code == FUNC_WRITE_SINGLE_REGISTER ||
        func_code == FUNC_WRITE_MULTIPLE_REGISTERS ||
        func_code == FUNC_READ_WRITE_MULTIPLE_REGISTERS) {
        append += mbuf_append_16(&s_modbus->transmit_buffer, s_modbus->write_address_u16);
        append += mbuf_append_16(&s_modbus->transmit_buffer, s_modbus->write_qty_u16);
    }

    if (func_code == FUNC_WRITE_MULTIPLE_COILS ||
        func_code == FUNC_WRITE_MULTIPLE_REGISTERS ||
        func_code == FUNC_READ_WRITE_MULTIPLE_REGISTERS) {
        append += mbuf_append(&s_modbus->transmit_buffer, &s_modbus->write_data_len, sizeof(uint8_t));
        append += mbuf_append_and_free(&s_modbus->transmit_buffer, s_modbus->write_data, s_modbus->write_data_len);
        LOG(LL_DEBUG, ("SlaveID: %.2x, Function: %.2x - Set Transmit Buffer", s_modbus->slave_id_u8, s_modbus->func_code_u8));
    }

    if (func_code == FUNC_MASK_WRITE_REGISTER) {
        append += mbuf_append_16(&s_modbus->transmit_buffer, s_modbus->read_address_u16);
        append += mbuf_append_16(&s_modbus->transmit_buffer, s_modbus->mask_and);
        append += mbuf_append_16(&s_modbus->transmit_buffer, s_modbus->mask_or);
    }

    append += mbuf_append_16(&s_modbus->transmit_buffer, calculate_crc16(s_modbus->transmit_buffer));

    LOG(LL_DEBUG, ("SlaveID: %.2x, Function: %.2x - Transmit Buffer %d",
                   s_modbus->slave_id_u8, s_modbus->func_code_u8, s_modbus->transmit_buffer.len));
    print_buffer(s_modbus->transmit_buffer);

    return append;
}

static bool start_transaction() {
    mbuf_clear(&s_modbus->receive_buffer);
    s_modbus->resp_status_u8 = 0x00;

    if (s_modbus->read_state == DISABLED && s_modbus->transmit_buffer.len > 0) {
        LOG(LL_DEBUG, ("SlaveID: %.2x, Function: %.2x - Modbus Transaction Start", s_modbus->slave_id_u8, s_modbus->func_code_u8));
        s_modbus->read_state = READ_START;
        mgos_uart_flush(s_modbus->uart_no);
        mgos_msleep(30);  //TODO delay for 3.5 Characters length according to baud rate
        req_timer = mgos_set_timer(mgos_sys_config_get_modbus_timeout(), 0, req_timeout_cb, NULL);
        mgos_uart_write(s_modbus->uart_no, s_modbus->transmit_buffer.buf, s_modbus->transmit_buffer.len);
        mgos_uart_set_rx_enabled(s_modbus->uart_no, true);
        mgos_uart_set_dispatcher(s_modbus->uart_no, uart_cb, &req_timer);
        return true;
    }
    return false;
}

/*
Read coils from modbus slave, callback function is called when response from modbus is completed or timed out.
*/
bool mb_read_coils(uint8_t slave_id, uint16_t read_address, uint16_t read_qty, mb_response_callback cb, void* cb_arg) {
    LOG(LL_DEBUG, ("Read Coils, Address: %.2x", read_address));
    if (!init_modbus(slave_id, FUNC_READ_COILS, 5, cb, cb_arg))
        return false;
    set_read_address(read_address, read_qty);

    if (!set_transmit_buffer())
        return false;
    return start_transaction();
}

/*
Read discrete inputs from modbus slave, callback function is called when response from modbus is completed or timed out.
*/
bool mb_read_discrete_inputs(uint8_t slave_id, uint16_t read_address, uint16_t read_qty, mb_response_callback cb, void* cb_arg) {
    LOG(LL_DEBUG, ("Read Discrete Inputs, Address: %.2x", read_address));
    if (!init_modbus(slave_id, FUNC_READ_DISCRETE_INPUTS, 5, cb, cb_arg))
        return false;
    set_read_address(read_address, read_qty);

    if (!set_transmit_buffer())
        return false;
    return start_transaction();
}

/*
Read holding registers from modbus slave, callback function is called when response from modbus is completed or timed out.
*/
bool mb_read_holding_registers(uint8_t slave_id, uint16_t read_address, uint16_t read_qty, mb_response_callback cb, void* cb_arg) {
    LOG(LL_DEBUG, ("Read Holding Registers, Address: %.2x", read_address));
    if (!init_modbus(slave_id, FUNC_READ_HOLDING_REGISTERS, 5, cb, cb_arg))
        return false;
    set_read_address(read_address, read_qty);

    if (!set_transmit_buffer())
        return false;
    return start_transaction();
}

/*
Read input registers from modbus slave, callback function is called when response from modbus is completed or timed out.
*/
bool mb_read_input_registers(uint8_t slave_id, uint16_t read_address, uint16_t read_qty, mb_response_callback cb, void* cb_arg) {
    LOG(LL_DEBUG, ("Read Input Registers, Address: %.2x", read_address));
    if (!init_modbus(slave_id, FUNC_READ_INPUT_REGISTERS, 5, cb, cb_arg))
        return false;
    set_read_address(read_address, read_qty);

    if (!set_transmit_buffer())
        return false;
    return start_transaction();
}

/*
Write coil in modbus slave, callback function is called when response from modbus is completed or timed out.
*/
bool mb_write_single_coil(uint8_t slave_id, uint16_t write_address, uint16_t write_value, mb_response_callback cb, void* cb_arg) {
    LOG(LL_DEBUG, ("Write Single Coil, Address: %.2x", write_address));
    if (!init_modbus(slave_id, FUNC_WRITE_SINGLE_COIL, 4, cb, cb_arg))
        return false;
    set_write_address(write_address, write_value, NULL, 0);

    if (!set_transmit_buffer())
        return false;
    return start_transaction();
}

/*
Write register in modbus slave, callback function is called when response from modbus is completed or timed out.
*/
bool mb_write_single_register(uint8_t slave_id, uint16_t write_address, uint16_t write_value, mb_response_callback cb, void* cb_arg) {
    LOG(LL_DEBUG, ("Write Single Register, Address: %.2x", write_address));
    if (!init_modbus(slave_id, FUNC_WRITE_SINGLE_REGISTER, 4, cb, cb_arg))
        return false;
    set_write_address(write_address, write_value, NULL, 0);

    if (!set_transmit_buffer())
        return false;
    return start_transaction();
}

/*
Write coils in modbus slave, callback function is called when response from modbus is completed or timed out.
*/
bool mb_write_multiple_coils(uint8_t slave_id, uint16_t write_address, uint16_t write_qty,
                             uint8_t* data, uint8_t len, mb_response_callback cb, void* cb_arg) {
    LOG(LL_DEBUG, ("Write Multiple Coils, Address: %.2x", write_address));
    if (!init_modbus(slave_id, FUNC_WRITE_MULTIPLE_COILS, 4, cb, cb_arg))
        return false;
    set_write_address(write_address, write_qty, data, len);

    if (!set_transmit_buffer())
        return false;
    return start_transaction();
}

/*
Write registers in modbus slave, callback function is called when response from modbus is completed or timed out.
*/
bool mb_write_multiple_registers(uint8_t slave_id, uint16_t write_address, uint16_t write_qty,
                                 uint8_t* data, uint8_t len, mb_response_callback cb, void* cb_arg) {
    LOG(LL_DEBUG, ("Write Multiple Registers, Address: %.2x", write_address));
    if (!init_modbus(slave_id, FUNC_WRITE_MULTIPLE_REGISTERS, 4, cb, cb_arg))
        return false;
    set_write_address(write_address, write_qty, data, len);

    if (!set_transmit_buffer())
        return false;
    return start_transaction();
}

/*
Read and write mulitple registers in modbus slave, callback function is called when response from modbus is completed or timed out.
*/
bool mb_read_write_multiple_registers(uint8_t slave_id, uint16_t read_address, uint16_t read_qty,
                                      uint16_t write_address, uint16_t write_qty,
                                      uint8_t* data, uint8_t len, mb_response_callback cb, void* cb_arg) {
    LOG(LL_DEBUG, ("Read Write Multiple Registers, Address: %.2x", write_address));
    if (!init_modbus(slave_id, FUNC_READ_WRITE_MULTIPLE_REGISTERS, 4, cb, cb_arg))
        return false;
    set_read_address(read_address, read_qty);
    set_write_address(write_address, write_qty, data, len);
    if (!set_transmit_buffer())
        return false;
    return start_transaction();
}

/*
Mask register in modbus slave, callback function is called when response from modbus is completed or timed out.
*/
bool mb_mask_write_register(uint8_t slave_id, uint16_t address, uint16_t and_mask, uint16_t or_mask, mb_response_callback cb, void* cb_arg) {
    LOG(LL_DEBUG, ("Mask Write Register, Address: %.2x", address));
    if (!init_modbus(slave_id, FUNC_MASK_WRITE_REGISTER, 4, cb, cb_arg))
        return false;
    s_modbus->read_address_u16 = address;
    s_modbus->mask_and = and_mask;
    s_modbus->mask_or = or_mask;

    if (!set_transmit_buffer())
        return false;
    return start_transaction();
}

bool mgos_modbus_create(const struct mgos_config_modbus* cfg) {
    struct mgos_uart_config ucfg;
    mgos_uart_config_set_defaults(cfg->uart_no, &ucfg);
    ucfg.baud_rate = mgos_sys_config_get_modbus_baudrate();
    if (mgos_sys_config_get_modbus_uart_rx_pin() >= 0) {
        ucfg.dev.rx_gpio = mgos_sys_config_get_modbus_uart_rx_pin();
    }
    if (mgos_sys_config_get_modbus_uart_tx_pin() >= 0) {
        ucfg.dev.tx_gpio = mgos_sys_config_get_modbus_uart_tx_pin();
    }
    if (mgos_sys_config_get_modbus_tx_en_gpio() >= 0) {
        ucfg.dev.tx_en_gpio = mgos_sys_config_get_modbus_tx_en_gpio();
    }
    if (mgos_sys_config_get_modbus_parity() >= 0 && mgos_sys_config_get_modbus_parity() < 3) {
        ucfg.parity = mgos_sys_config_get_modbus_parity();
    }
    if (mgos_sys_config_get_modbus_stop_bits() > 0 && mgos_sys_config_get_modbus_stop_bits() < 4) {
        ucfg.stop_bits = mgos_sys_config_get_modbus_stop_bits();
    }
    ucfg.dev.hd = mgos_sys_config_get_modbus_tx_en_enable();
    ucfg.dev.tx_en_gpio_val = mgos_sys_config_get_modbus_tx_en_gpio_val();

    char b1[8], b2[8], b3[8];
    LOG(LL_DEBUG, ("MODBUS UART%d (RX:%s TX:%s TX_EN:%s), Baudrate %d, Parity %d, Stop bits %d, Half Duplex %d, tx_en Value %d",
                   cfg->uart_no, mgos_gpio_str(ucfg.dev.rx_gpio, b1),
                   mgos_gpio_str(ucfg.dev.tx_gpio, b2),
                   mgos_gpio_str(ucfg.dev.tx_en_gpio, b3), ucfg.baud_rate,
                   ucfg.parity, ucfg.stop_bits, ucfg.dev.hd, ucfg.dev.tx_en_gpio_val));

    if (!mgos_uart_configure(cfg->uart_no, &ucfg)) {
        LOG(LL_ERROR, ("Failed to configure UART%d", cfg->uart_no));
        return false;
    }

    s_modbus = (struct mgos_modbus*)calloc(1, sizeof(*s_modbus));
    if (s_modbus == NULL)
        return false;
    mbuf_init(&s_modbus->transmit_buffer, 300);
    mbuf_init(&s_modbus->receive_buffer, 300);
    s_modbus->read_state = DISABLED;
    s_modbus->uart_no = cfg->uart_no;

    return true;
}

// New parse functions, all except char reversed to account for ESP32 little endianness
signed char parse_value_signed_char(uint8_t *strt_ptr,
                                    enum MB_VALUE_BYTEORDER byte_order) {
  union {
    uint8_t orderedBytes[1];
    signed char parsedValue;
  } u;
  if (byte_order == MAP_BYTEORDER_A) {
    u.orderedBytes[0] = strt_ptr[0];
  } else if (byte_order == MAP_BYTEORDER_B) {
    u.orderedBytes[0] = strt_ptr[1];
  } else {
    u.orderedBytes[0] = 0xFF;
  }
  return u.parsedValue;
}

unsigned char parse_value_unsigned_char(uint8_t *strt_ptr,
                                        enum MB_VALUE_BYTEORDER byte_order) {
  union {
    uint8_t orderedBytes[1];
    unsigned char parsedValue;
  } u;
  if (byte_order == MAP_BYTEORDER_A) {
    u.orderedBytes[0] = strt_ptr[0];
  } else if (byte_order == MAP_BYTEORDER_B) {
    u.orderedBytes[0] = strt_ptr[1];
  } else {
    u.orderedBytes[0] = 0xFF;
  }
  return u.parsedValue;
}

signed int parse_value_signed_int(uint8_t *strt_ptr,
                                  enum MB_VALUE_BYTEORDER byte_order) {
  union {
    uint8_t orderedBytes[2];
    signed int parsedValue;
  } u;
  if (byte_order == MAP_BYTEORDER_AB) {
    u.orderedBytes[0] = strt_ptr[1];
    u.orderedBytes[1] = strt_ptr[0];
  } else if (byte_order == MAP_BYTEORDER_BA) {
    u.orderedBytes[0] = strt_ptr[0];
    u.orderedBytes[1] = strt_ptr[1];
  } else {
    u.orderedBytes[0] = 0xFF;
    u.orderedBytes[1] = 0xFF;
  }
  return u.parsedValue;
}

unsigned int parse_value_unsigned_int(uint8_t *strt_ptr,
                                      enum MB_VALUE_BYTEORDER byte_order) {
  union {
    uint8_t orderedBytes[2];
    unsigned int parsedValue;
  } u;
  if (byte_order == MAP_BYTEORDER_AB) {
    u.orderedBytes[0] = strt_ptr[1];
    u.orderedBytes[1] = strt_ptr[0];
  } else if (byte_order == MAP_BYTEORDER_BA) {
    u.orderedBytes[0] = strt_ptr[0];
    u.orderedBytes[1] = strt_ptr[1];
  } else {
    u.orderedBytes[0] = 0xFF;
    u.orderedBytes[1] = 0xFF;
  }
  return u.parsedValue;
}

signed long parse_value_signed_long(uint8_t *strt_ptr,
                                    enum MB_VALUE_BYTEORDER byte_order) {
  union {
    uint8_t orderedBytes[4];
    signed long parsedValue;
  } u;
  if (byte_order == MAP_BYTEORDER_ABCD) {
    u.orderedBytes[0] = strt_ptr[3];
    u.orderedBytes[1] = strt_ptr[2];
    u.orderedBytes[2] = strt_ptr[1];
    u.orderedBytes[3] = strt_ptr[0];
  } else if (byte_order == MAP_BYTEORDER_DCBA) {
    u.orderedBytes[0] = strt_ptr[0];
    u.orderedBytes[1] = strt_ptr[1];
    u.orderedBytes[2] = strt_ptr[2];
    u.orderedBytes[3] = strt_ptr[3];
  } else if (byte_order == MAP_BYTEORDER_BADC) {
    u.orderedBytes[0] = strt_ptr[2];
    u.orderedBytes[1] = strt_ptr[3];
    u.orderedBytes[2] = strt_ptr[0];
    u.orderedBytes[3] = strt_ptr[1];
  } else if (byte_order == MAP_BYTEORDER_CDAB) {
    u.orderedBytes[0] = strt_ptr[1];
    u.orderedBytes[1] = strt_ptr[0];
    u.orderedBytes[2] = strt_ptr[3];
    u.orderedBytes[3] = strt_ptr[2];
  } else {
    u.orderedBytes[0] = 0xFF;
    u.orderedBytes[1] = 0xFF;
    u.orderedBytes[2] = 0xFF;
    u.orderedBytes[3] = 0xFF;
  }
  return u.parsedValue;
}

unsigned long parse_value_unsigned_long(uint8_t *strt_ptr,
                                        enum MB_VALUE_BYTEORDER byte_order) {
  union {
    uint8_t orderedBytes[4];
    unsigned long parsedValue;
  } u;
  if (byte_order == MAP_BYTEORDER_ABCD) {
    u.orderedBytes[0] = strt_ptr[3];
    u.orderedBytes[1] = strt_ptr[2];
    u.orderedBytes[2] = strt_ptr[1];
    u.orderedBytes[3] = strt_ptr[0];
  } else if (byte_order == MAP_BYTEORDER_DCBA) {
    u.orderedBytes[0] = strt_ptr[0];
    u.orderedBytes[1] = strt_ptr[1];
    u.orderedBytes[2] = strt_ptr[2];
    u.orderedBytes[3] = strt_ptr[3];
  } else if (byte_order == MAP_BYTEORDER_BADC) {
    u.orderedBytes[0] = strt_ptr[2];
    u.orderedBytes[1] = strt_ptr[3];
    u.orderedBytes[2] = strt_ptr[0];
    u.orderedBytes[3] = strt_ptr[1];
  } else if (byte_order == MAP_BYTEORDER_CDAB) {
    u.orderedBytes[0] = strt_ptr[1];
    u.orderedBytes[1] = strt_ptr[0];
    u.orderedBytes[2] = strt_ptr[3];
    u.orderedBytes[3] = strt_ptr[2];
  } else {
    u.orderedBytes[0] = 0xFF;
    u.orderedBytes[1] = 0xFF;
    u.orderedBytes[2] = 0xFF;
    u.orderedBytes[3] = 0xFF;
  }
  return u.parsedValue;
}
float parse_value_float(uint8_t *strt_ptr, enum MB_VALUE_BYTEORDER byte_order) {
  union {
    uint8_t orderedBytes[4];
    float parsedValue;
  } u;
  if (byte_order == MAP_BYTEORDER_ABCD) {
    u.orderedBytes[0] = strt_ptr[3];
    u.orderedBytes[1] = strt_ptr[2];
    u.orderedBytes[2] = strt_ptr[1];
    u.orderedBytes[3] = strt_ptr[0];
  } else if (byte_order == MAP_BYTEORDER_DCBA) {
    u.orderedBytes[0] = strt_ptr[0];
    u.orderedBytes[1] = strt_ptr[1];
    u.orderedBytes[2] = strt_ptr[2];
    u.orderedBytes[3] = strt_ptr[3];
  } else if (byte_order == MAP_BYTEORDER_BADC) {
    u.orderedBytes[0] = strt_ptr[2];
    u.orderedBytes[1] = strt_ptr[3];
    u.orderedBytes[2] = strt_ptr[0];
    u.orderedBytes[3] = strt_ptr[1];
  } else if (byte_order == MAP_BYTEORDER_CDAB) {
    u.orderedBytes[0] = strt_ptr[1];
    u.orderedBytes[1] = strt_ptr[0];
    u.orderedBytes[2] = strt_ptr[3];
    u.orderedBytes[3] = strt_ptr[2];
  } else {
    u.orderedBytes[0] = 0xFF;
    u.orderedBytes[1] = 0xFF;
    u.orderedBytes[2] = 0xFF;
    u.orderedBytes[3] = 0xFF;
  }
  return u.parsedValue;
}
unsigned long long parse_value_unsigned_longlong(
    uint8_t *strt_ptr, enum MB_VALUE_BYTEORDER byte_order) {
  union {
    uint8_t orderedBytes[8];
    float parsedValue;
  } u;
  if (byte_order == MAP_BYTEORDER_ABCDEFGH) {
    u.orderedBytes[0] = strt_ptr[7];
    u.orderedBytes[1] = strt_ptr[6];
    u.orderedBytes[2] = strt_ptr[5];
    u.orderedBytes[3] = strt_ptr[4];
    u.orderedBytes[4] = strt_ptr[3];
    u.orderedBytes[5] = strt_ptr[2];
    u.orderedBytes[6] = strt_ptr[1];
    u.orderedBytes[7] = strt_ptr[0];
  } else if (byte_order == MAP_BYTEORDER_HGFEDCBA) {
    u.orderedBytes[0] = strt_ptr[0];
    u.orderedBytes[1] = strt_ptr[1];
    u.orderedBytes[2] = strt_ptr[2];
    u.orderedBytes[3] = strt_ptr[3];
    u.orderedBytes[4] = strt_ptr[4];
    u.orderedBytes[5] = strt_ptr[5];
    u.orderedBytes[6] = strt_ptr[6];
    u.orderedBytes[7] = strt_ptr[7];
  } else {
    u.orderedBytes[0] = 0xFF;
    u.orderedBytes[1] = 0xFF;
    u.orderedBytes[2] = 0xFF;
    u.orderedBytes[3] = 0xFF;
    u.orderedBytes[4] = 0xFF;
    u.orderedBytes[5] = 0xFF;
    u.orderedBytes[6] = 0xFF;
    u.orderedBytes[7] = 0xFF;
  }
  return u.parsedValue;
}

struct mb_variable parse_address_info(struct json_token address_info, int* address) {
    *address = -1;
    struct mb_variable valueProperties = {NULL,NULL,0,0,MAP_BYTEORDER_A, MAP_TYPE_CHAR_SIGNED};

    json_scanf(address_info.ptr, address_info.len, "%d", address);
    if (*address < 0) {
        char *type_temp = NULL;
        char *order_temp = NULL;
        json_scanf(address_info.ptr, address_info.len, "{add: %d, order: %Q, type: %Q}", address, &order_temp, &type_temp);
        if (type_temp != NULL) {
            if (strcmp(type_temp, "char_signed") == 0) {
                valueProperties.type = MAP_TYPE_CHAR_SIGNED;
                valueProperties.registers = 1;
            } else if (strcmp(type_temp, "char_unsigned") == 0) {
                valueProperties.type = MAP_TYPE_CHAR_UNSIGNED;
                valueProperties.registers = 1;
            } else if (strcmp(type_temp, "int_signed") == 0) {
                valueProperties.type = MAP_TYPE_INT_SIGNED;
                valueProperties.registers = 1;
            } else if (strcmp(type_temp, "int_unsigned") == 0) {
                valueProperties.type = MAP_TYPE_INT_UNSIGNED;
                valueProperties.registers = 1;
            } else if (strcmp(type_temp, "long_signed") == 0) {
                valueProperties.type = MAP_TYPE_LONG_SIGNED;
                valueProperties.registers = 2;
            } else if (strcmp(type_temp, "long_unsigned") == 0) {
                valueProperties.type = MAP_TYPE_LONG_UNSIGNED;
                valueProperties.registers = 2;
            } else if (strcmp(type_temp, "longlong_unsigned") == 0) {
                valueProperties.type = MAP_TYPE_LONGLONG_UNSIGNED;
                valueProperties.registers = 4;
            } else if (strcmp(type_temp, "float") == 0) {
                valueProperties.type = MAP_TYPE_FLOAT;
                valueProperties.registers = 2;
            }
            free(type_temp);
        }
        if (order_temp != NULL) {
            if (strcmp(order_temp, "a") == 0) {
                valueProperties.order = MAP_BYTEORDER_A;
            } else if (strcmp(order_temp, "b") == 0) {
                valueProperties.order = MAP_BYTEORDER_B;
            } else if (strcmp(order_temp, "ab") == 0) {
                valueProperties.order = MAP_BYTEORDER_AB;
            } else if (strcmp(order_temp, "ba") == 0) {
                valueProperties.order = MAP_BYTEORDER_BA;
            } else if (strcmp(order_temp, "abcd") == 0) {
                valueProperties.order = MAP_BYTEORDER_ABCD;
            } else if (strcmp(order_temp, "dcba") == 0) {
                valueProperties.order = MAP_BYTEORDER_DCBA;
            } else if (strcmp(order_temp, "badc") == 0) {
                valueProperties.order = MAP_BYTEORDER_BADC;
            } else if (strcmp(order_temp, "cdab") == 0) {
                valueProperties.order = MAP_BYTEORDER_CDAB;
            } else if (strcmp(order_temp, "abcdefgh") == 0) {
                valueProperties.order = MAP_BYTEORDER_ABCDEFGH;
            } else if (strcmp(order_temp, "hgfedcba") == 0) {
                valueProperties.order = MAP_BYTEORDER_HGFEDCBA;
            }
            free(order_temp);
        }
    }
    return valueProperties;
}

int get_buffer_offset(uint16_t read_start_address, uint8_t byte_count, uint16_t required_address) {
    int read_qty = byte_count / 2;
    int max_read_address = read_start_address + read_qty - 1;
    if (required_address < read_start_address || required_address > max_read_address) {
        LOG(LL_INFO, ("Invalid address: %d, address out of range, start address - %d, byte count - %d",
                      required_address, read_start_address, byte_count));
        return -1;
    }
    int diff = required_address - read_start_address;
    return diff * 2 + 3;
}

//Caller needs to free the returned attribute value string
char* get_attribute_value(struct mbuf* mb_reponse, uint16_t read_start_address, struct json_token attr_info) {
    int required_address = -1;
    struct mb_variable valueProperties = parse_address_info(attr_info, &required_address);
    if (required_address < 0) {
        LOG(LL_INFO, ("Cannot find address in modbus response"));
        return NULL;
    }

    int offset = get_buffer_offset(read_start_address, (uint8_t)mb_reponse->buf[2], required_address);
    LOG(LL_DEBUG, ("Attribute info - offset: %d, address: %d, type: %d, order: %d", offset, required_address, valueProperties.type, valueProperties.order));
    if (offset < 0) {
        return NULL;
    }
    uint8_t* start_position = (uint8_t*)mb_reponse->buf + offset;
    char* res = NULL;
    switch (valueProperties.type) {
        case MAP_TYPE_CHAR_SIGNED:
            mg_asprintf(&res, 0, "%d", parse_value_signed_char(start_position, valueProperties.order));
            break;
        case MAP_TYPE_CHAR_UNSIGNED:
            mg_asprintf(&res, 0, "%u", parse_value_unsigned_char(start_position, valueProperties.order));
            break;
        case MAP_TYPE_INT_SIGNED:
            mg_asprintf(&res, 0, "%d", parse_value_signed_int(start_position, valueProperties.order));
            break;
        case MAP_TYPE_INT_UNSIGNED:
            mg_asprintf(&res, 0, "%u", parse_value_unsigned_int(start_position, valueProperties.order));
            break;
        case MAP_TYPE_LONG_SIGNED:
            mg_asprintf(&res, 0, "%ld", parse_value_signed_long(start_position, valueProperties.order));
            break;
        case MAP_TYPE_LONG_UNSIGNED:
            mg_asprintf(&res, 0, "%lu", parse_value_unsigned_long(start_position, valueProperties.order));
            break;
        case MAP_TYPE_LONGLONG_UNSIGNED:
            mg_asprintf(&res, 0, "%llu", parse_value_unsigned_longlong(start_position, valueProperties.order));
            break;
        case MAP_TYPE_FLOAT:
            mg_asprintf(&res, 0, "%f", parse_value_float(start_position, valueProperties.order));
            break;
        case MAP_TYPE_HEX:
        default:
            mg_asprintf(&res, 0, "\"0x%.2x%.2x%.2x%.2x\"", *start_position,
                        *(start_position + 1), *(start_position + 2), *(start_position + 3));
            break;
    }
    return res;
}

bool set_resp_json(struct mbuf* json_buf, const char* key,
                    int key_len, const char* value, int value_len) {
    if (json_buf == NULL || key == NULL || value == NULL) {
        return false;
    }
    int p = 0;
    if (json_buf->len == 0) {
        if ((p = mbuf_insert(json_buf, 0, "{}", 3)) <= 0) {
            return false;
        }
    } else {
        if ((p = mbuf_insert(json_buf, json_buf->len-2, ",", 1)) <= 0) {
            return false;
        }
    }
    char* kv = NULL;
    mg_asprintf(&kv, 0, "\"%.*s\":%.*s", key_len, key, value_len, value);
    if (kv == NULL) {
        return false;
    }
    if ((p = mbuf_insert(json_buf,  json_buf->len-2, kv, strlen(kv))) <= 0) {
        return false;
    }
    free(kv);
    return true;
}

char* mb_map_register_response(const char* json_map, struct mbuf* mb_resp, struct mb_request_info* info) {
    LOG(LL_INFO, ("Map modbus response to json"));
    void* h = NULL;
    struct json_token attr_name, attr_info;
    struct mbuf resp_buf;
    mbuf_init(&resp_buf, strlen(json_map) * 2);
    while ((h = json_next_key(json_map, strlen(json_map), h, ".", &attr_name, &attr_info)) != NULL) {
        char* attr_value = NULL;
        if ((attr_value = get_attribute_value(mb_resp, info->read_address, attr_info)) != NULL) {
            LOG(LL_VERBOSE_DEBUG, ("Attribute value for %.*s: %s", attr_name.len, attr_name.ptr, attr_value));
            if (!set_resp_json(&resp_buf, attr_name.ptr, attr_name.len, attr_value, strlen(attr_value))) {
                LOG(LL_ERROR, ("Unable to create modbus mapped response json"));
                mbuf_free(&resp_buf);
                return NULL;
            }
            free(attr_value);
        }
    }
    char* resp = NULL;
    if (resp_buf.len > 0) {
        resp = strndup(resp_buf.buf, resp_buf.len);
    }
    mbuf_free(&resp_buf);
    return resp;
}

char* mb_map_register_responsef(const char* json_file, struct mbuf* mb_resp, struct mb_request_info* info) {
    char* map_str = json_fread(json_file);
    if (map_str == NULL) {
        LOG(LL_ERROR, ("Error reading modbus json map file"));
        return NULL;
    }
    char* resp = mb_map_register_response(map_str, mb_resp, info);
    free(map_str);
    return resp;
}

bool mgos_modbus_connect() {
    if (!mgos_sys_config_get_modbus_enable()) {
        return false;
    }
    struct mgos_config_modbus* cfg = &mgos_sys_config.modbus;
    struct mgos_uart_config ucfg;
    char b1[8], b2[8], b3[8];
    mgos_uart_config_get(mgos_sys_config.modbus.uart_no, &ucfg);
    if (!mgos_uart_configure(cfg->uart_no, &ucfg)) {
        LOG(LL_ERROR, ("Failed to configure UART%d", cfg->uart_no));
        return false;
    }
    LOG(LL_DEBUG, ("MODBUS UART%d (RX:%s TX:%s TX_EN:%s), Baudrate %d, Parity %d, Stop bits %d, Half Duplex %d, tx_en Value %d",
                   cfg->uart_no, mgos_gpio_str(ucfg.dev.rx_gpio, b1),
                   mgos_gpio_str(ucfg.dev.tx_gpio, b2),
                   mgos_gpio_str(ucfg.dev.tx_en_gpio, b3), ucfg.baud_rate,
                   ucfg.parity, ucfg.stop_bits, ucfg.dev.hd, ucfg.dev.tx_en_gpio_val));
    return true;
}

char* mb_resp_to_str(struct mbuf response) {
    int len = response.len * 2 + 3;
    int resp_count = 0;
    char* resp = malloc(len);
    memset(resp, '\0', len);
    resp_count += sprintf(resp + resp_count, "\"");
    for (size_t i = 0; i < response.len && resp_count < len; i++) {
        resp_count += sprintf(resp + resp_count, "%.2x", response.buf[i]);
    }
    resp_count += sprintf(resp + resp_count, "\"");
    return resp;
}

/*
void rpc_mb_resp_cb(uint8_t status, struct mb_request_info info, struct mbuf response, void* param) {
    struct mb_job* rpc_i = (struct mb_job*)param;
    char* resp = NULL;
    LOG(LL_INFO, ("Modbus.Read rpc response, status: %#02x", status));
    if (status == RESP_SUCCESS) {
        if (rpc_i->map != NULL) {
            resp = mb_map_register_response(rpc_i->map, &response, &info);
        } else if (rpc_i->map_file != NULL) {
            resp = mb_map_register_responsef(rpc_i->map_file, &response, &info);
        } else {
            resp = mb_resp_to_str(response);
        }
    } else {
        resp = mb_resp_to_str(response);
    }

    if (resp == NULL) {
        mg_rpc_send_errorf(rpc_i->ri, 400, "Invalid json map");
    } else {
        mg_rpc_send_responsef(rpc_i->ri, "{resp_code:%d, data:%s}", status, resp);
    }

    free(resp);
    free(rpc_i->map);
    free(rpc_i->map_file);
    free(rpc_i);
}


static void rpc_modbus_read_handler(struct mg_rpc_request_info* ri, void* cb_arg,
                                    struct mg_rpc_frame_info* fi, struct mg_str args) {
    LOG(LL_INFO, ("Modbus.Read rpc called, payload: %.*s", args.len, args.p));
    int func = -1, id = -1, start = -1, qty = -1;
    char *map_file = NULL, *map = NULL;
    json_scanf(args.p, args.len, ri->args_fmt, &func, &id, &start, &qty, &map_file, &map);
    if (func <= 0) {
        mg_rpc_send_errorf(ri, 400, "Unsupported function code");
        goto out;
    }
    if (id <= 0) {
        mg_rpc_send_errorf(ri, 400, "Slave id is required");
        goto out;
    }
    if (start < 0) {
        mg_rpc_send_errorf(ri, 400, "Read start address is required");
        goto out;
    }
    if (qty <= 0) {
        mg_rpc_send_errorf(ri, 400, "Read quantity is required");
        goto out;
    }

    struct mb_job* rpc_i = malloc(sizeof(struct mb_job));
    rpc_i->ri = ri;
    rpc_i->map = map;
    rpc_i->map_file = map_file;

    bool resp = false;
    if ((uint8_t)func == FUNC_READ_COILS) {
        resp = mb_read_coils((uint8_t)id, (uint16_t)start, (uint16_t)qty, rpc_mb_resp_cb, rpc_i);
    } else if ((uint8_t)func == FUNC_READ_DISCRETE_INPUTS) {
        resp = mb_read_discrete_inputs((uint8_t)id, (uint16_t)start, (uint16_t)qty, rpc_mb_resp_cb, rpc_i);
    } else if ((uint8_t)func == FUNC_READ_HOLDING_REGISTERS) {
        resp = mb_read_holding_registers((uint8_t)id, (uint16_t)start, (uint16_t)qty, rpc_mb_resp_cb, rpc_i);
    } else if ((uint8_t)func == FUNC_READ_INPUT_REGISTERS) {
        resp = mb_read_input_registers((uint8_t)id, (uint16_t)start, (uint16_t)qty, rpc_mb_resp_cb, rpc_i);
    }
    if (!resp) {
        mg_rpc_send_errorf(ri, 400, "Unable to execute modbus request");
        free(map);
        free(map_file);
        free(rpc_i);
    }
out:
    (void)cb_arg;
    (void)fi;
    return;
}
*/
//Caller needs to free the returned attribute value string
char* mb_parse_variable_value(struct mbuf* mb_reponse, uint16_t read_start_address, struct mb_variable* mb_variable) {
    int required_address = mb_variable->address;
    int offset = get_buffer_offset(read_start_address, (uint8_t)mb_reponse->buf[2], required_address);
    LOG(LL_DEBUG, ("Attribute info - offset: %d, address: %d, type: %d, order: %d", offset, required_address, mb_variable->type, mb_variable->order));
    if (offset < 0) {
        return NULL;
    }
    uint8_t* start_position = (uint8_t*)mb_reponse->buf + offset;
    char* res = NULL;
    switch (mb_variable->type) {
        case MAP_TYPE_CHAR_SIGNED:
            mg_asprintf(&res, 0, "%d", parse_value_signed_char(start_position, mb_variable->order));
            break;
        case MAP_TYPE_CHAR_UNSIGNED:
            mg_asprintf(&res, 0, "%u", parse_value_unsigned_char(start_position, mb_variable->order));
            break;
        case MAP_TYPE_INT_SIGNED:
            mg_asprintf(&res, 0, "%d", parse_value_signed_int(start_position, mb_variable->order));
            break;
        case MAP_TYPE_INT_UNSIGNED:
            mg_asprintf(&res, 0, "%u", parse_value_unsigned_int(start_position, mb_variable->order));
            break;
        case MAP_TYPE_LONG_SIGNED:
            mg_asprintf(&res, 0, "%ld", parse_value_signed_long(start_position, mb_variable->order));
            break;
        case MAP_TYPE_LONG_UNSIGNED:
            mg_asprintf(&res, 0, "%lu", parse_value_unsigned_long(start_position, mb_variable->order));
            break;
        case MAP_TYPE_LONGLONG_UNSIGNED:
            mg_asprintf(&res, 0, "%llu", parse_value_unsigned_longlong(start_position, mb_variable->order));
            break;
        case MAP_TYPE_FLOAT:
            mg_asprintf(&res, 0, "%f", parse_value_float(start_position, mb_variable->order));
            break;
        case MAP_TYPE_HEX:
        default:
            mg_asprintf(&res, 0, "\"0x%.2x%.2x%.2x%.2x\"", *start_position,
                        *(start_position + 1), *(start_position + 2), *(start_position + 3));
            break;
    }
    return res;
}

bool mb_job_process_response(struct mbuf* mb_resp, struct mb_request* mb_request){ 
    for(int i=0;i<mb_request->variables_count;i++){
        struct mb_variable* variable = ((struct mb_variable*) mb_request->variables)+i;
        variable->value = mb_parse_variable_value(mb_resp, mb_request->start, variable);
        LOG(LL_DEBUG, ("Attribute value for %s: %s", variable->key, variable->value));
    
    }
    return true;
}

void mb_job_response_cb(uint8_t status, struct mb_request_info info, struct mbuf response, void* param);

bool mb_next_request(struct mb_job* job){
    bool resp = false;
    struct mb_request* read_jobs = job->requests;
    uint16_t current_job = job->requests_next-1;
    LOG(LL_DEBUG, ("Read Job No %d-> ID:%d Func:%d Start: %d Regs: %d",current_job+1,job->id,job->func,read_jobs[current_job].start,read_jobs[current_job].qty)); 

    if (job->func == FUNC_READ_COILS) {
        resp = mb_read_coils(job->id, read_jobs[current_job].start, read_jobs[current_job].qty, mb_job_response_cb, job);
    } else if (job->func == FUNC_READ_DISCRETE_INPUTS) {
        resp = mb_read_discrete_inputs(job->id, read_jobs[current_job].start, read_jobs[current_job].qty, mb_job_response_cb, job);
    } else if (job->func == FUNC_READ_HOLDING_REGISTERS) {
        resp = mb_read_holding_registers(job->id, read_jobs[current_job].start, read_jobs[current_job].qty, mb_job_response_cb, job);
    } else if (job->func == FUNC_READ_INPUT_REGISTERS) {
        resp = mb_read_input_registers(job->id, read_jobs[current_job].start, read_jobs[current_job].qty, mb_job_response_cb, job);
    }
    return resp;
}

//Return heap allocated JSON string with job results
char* mb_job_result_json(struct mb_job* job){
    struct mb_request* requests = job->requests;
    struct mbuf fullResponse;
    mbuf_init(&fullResponse,sizeof(char));
    int offset = 0;
    offset += mbuf_append(&fullResponse,"{",1);
    for(uint16_t i=0;i<job->requests_count;i++){
        for(uint16_t j=0;j<(requests+i)->variables_count;j++){
            if ((((requests+i)->variables)+j)->key != NULL && (((requests+i)->variables)+j)->value != NULL)
            offset+= mbuf_append(&fullResponse,"\"",1);
            offset+= mbuf_append(&fullResponse,(((requests+i)->variables)+j)->key,strlen((((requests+i)->variables)+j)->key));
            offset+= mbuf_append(&fullResponse,"\":\"",3);
            offset+= mbuf_append(&fullResponse,(((requests+i)->variables)+j)->value,strlen((((requests+i)->variables)+j)->value));
            offset+= mbuf_append(&fullResponse,"\"",1);
            if (j<((requests+i)->variables_count)-1){
                offset+= mbuf_append(&fullResponse,",",1);
            }
        }   
    }
    offset+= mbuf_append(&fullResponse,"}",2);
    char* result = (char*)malloc(fullResponse.len);
    strcpy(result,fullResponse.buf);
    mbuf_free(&fullResponse);
    return result;
}

//safely recursively frees mb_job
void mb_free_job(struct mb_job* job){
    for(uint16_t i=0;i<job->requests_count;i++){
        struct mb_request* the_request  = (job->requests)+i;
        for(uint16_t j=0;j<the_request->variables_count;j++){
            struct mb_variable* the_variable = (the_request->variables)+j;
            if (the_variable->key) free(the_variable->key);
            if (the_variable->value) free(the_variable->value);
        }
        if (the_request->variables) free(the_request->variables);
    }
    if (job->requests) free(job->requests);
    if (job->map)  free(job->map);
    if (job) free(job);
}

void rpc_send_job_response_cb(struct mb_job* job, bool error, uint8_t code, char* msg){
    if (!error){
        char* result =  mb_job_result_json(job);
        LOG(LL_INFO, ("Sent response: %s", result));
        mg_rpc_send_responsef((struct mg_rpc_request_info*) job->finished_job_cb_param, "{resp_code:%d, data:%s}", code, result);
    }else{
        LOG(LL_INFO, ("Sent error: %d message:%s", code, msg));
        mg_rpc_send_errorf((struct mg_rpc_request_info*) job->finished_job_cb_param, 400, "Error, code:%d message:%s", code, msg);
    }
    mb_free_job(job);
}

//does not free job!
void mb_job_response_cb(uint8_t status, struct mb_request_info info, struct mbuf response, void* param) {
    struct mb_job* job = (struct mb_job*)param;
    struct mb_request* requests = job->requests;
    struct mb_request* finished_request = requests+(job->requests_next-1);
        
    job->requests_next--;
    LOG(LL_DEBUG, ("Finished Job with status: %#02x remaining: %d", status, job->requests_next));
    
    if (status == RESP_SUCCESS) {      
        if (!mb_job_process_response(&response,finished_request)) {
            LOG(LL_DEBUG, ("Map error"));
            char* resp;
            resp = mb_resp_to_str(response);
            char msg[strlen(resp)+20];
            strcpy(msg,"Map error, buffer: ");
            strcat(msg,resp);
            free(resp);
            (*((mb_finished_job_cb)job->finished_job_cb))(job,true,status,msg);
        } else if (job->requests_next>0) {
            LOG(LL_DEBUG, ("Still jobs to go"));
            if(!mb_next_request(job)){
               (*((mb_finished_job_cb)job->finished_job_cb))(job,true,status,"unable to execute next modbus request");
            };
            return;
        } else if (job->requests_next==0) {
            if(job->finished_job_cb!=NULL){
                (*((mb_finished_job_cb)job->finished_job_cb))(job,false,status,NULL);
            }
        }
    }else{
        LOG(LL_DEBUG, ("Modbus read error"));
        if(job->finished_job_cb!=NULL){
                (*((mb_finished_job_cb)job->finished_job_cb))(job,true,status,"Modbus read error");
        }
    }
}

int compare_modbus_variables(const void* p1, const void* p2){
    struct mb_variable* var1 = (struct mb_variable*) p1;
    struct mb_variable* var2 = (struct mb_variable*) p2;
    if(var1->address>var2->address){
        return 1;
    } else if (var1->address<var2->address){
        return -1;
    }else{
        return 0;
    }
}

bool mb_init_job(struct mb_job* job, char* map, char* keys){

    //Lookup keys in map and create mb_variable
    uint16_t variable_counter = 0;
    struct mbuf variable_buffer;
    mbuf_init(&variable_buffer,sizeof(struct mb_variable));
    void* h = NULL;
    int json_index;
    struct json_token json_value;
    
    while ((h = json_next_elem(keys, strlen(keys), h, "", &json_index, &json_value)) != NULL) {      
        char* searchpat;
        char json_key[json_value.len+1], *jsonkeyptr = json_key;
        json_key[json_value.len] = '\0';
        strncpy(jsonkeyptr,json_value.ptr,json_value.len);
        mg_asprintf(&searchpat,0,"{%s:%%T}",jsonkeyptr);       
        struct json_token res_token;
        if(json_scanf(map,strlen(map),searchpat,&res_token)>0){
            struct mb_variable current_variable;
            int addr_temp = 0;
            char *type_temp = NULL;
            char *order_temp = NULL;
            json_scanf(res_token.ptr, res_token.len, "{add: %d, order: %Q, type: %Q}", &addr_temp, &order_temp, &type_temp);
            LOG(LL_DEBUG, ("key:%s, address:%d, order:%s, type:%s",jsonkeyptr,addr_temp,order_temp,type_temp));
            current_variable.address = addr_temp;
            current_variable.key = strdup(jsonkeyptr);
            if (type_temp != NULL) {
                if (strcmp(type_temp, "char_signed") == 0) {
                    current_variable.type = MAP_TYPE_CHAR_SIGNED;
                    current_variable.registers = 1;
                } else if (strcmp(type_temp, "char_unsigned") == 0) {
                    current_variable.type = MAP_TYPE_CHAR_UNSIGNED;
                    current_variable.registers = 1;
                } else if (strcmp(type_temp, "int_signed") == 0) {
                    current_variable.type = MAP_TYPE_INT_SIGNED;
                    current_variable.registers = 1;
                } else if (strcmp(type_temp, "int_unsigned") == 0) {
                    current_variable.type = MAP_TYPE_INT_UNSIGNED;
                    current_variable.registers = 1;
                } else if (strcmp(type_temp, "long_signed") == 0) {
                    current_variable.type = MAP_TYPE_LONG_SIGNED;
                    current_variable.registers = 2;
                } else if (strcmp(type_temp, "long_unsigned") == 0) {
                    current_variable.type = MAP_TYPE_LONG_UNSIGNED;
                    current_variable.registers = 2;
                } else if (strcmp(type_temp, "longlong_unsigned") == 0) {
                    current_variable.type = MAP_TYPE_LONGLONG_UNSIGNED;
                    current_variable.registers = 4;
                } else if (strcmp(type_temp, "float") == 0) {
                    current_variable.type = MAP_TYPE_FLOAT;
                    current_variable.registers = 2;
                }
                free(type_temp);
            }
            if (order_temp != NULL) {
                if (strcmp(order_temp, "a") == 0) {
                    current_variable.order = MAP_BYTEORDER_A;
                } else if (strcmp(order_temp, "b") == 0) {
                    current_variable.order = MAP_BYTEORDER_B;
                } else if (strcmp(order_temp, "ab") == 0) {
                    current_variable.order = MAP_BYTEORDER_AB;
                } else if (strcmp(order_temp, "ba") == 0) {
                    current_variable.order = MAP_BYTEORDER_BA;
                } else if (strcmp(order_temp, "abcd") == 0) {
                    current_variable.order = MAP_BYTEORDER_ABCD;
                } else if (strcmp(order_temp, "dcba") == 0) {
                    current_variable.order = MAP_BYTEORDER_DCBA;
                } else if (strcmp(order_temp, "badc") == 0) {
                    current_variable.order = MAP_BYTEORDER_BADC;
                } else if (strcmp(order_temp, "cdab") == 0) {
                    current_variable.order = MAP_BYTEORDER_CDAB;
                } else if (strcmp(order_temp, "abcdefgh") == 0) {
                    current_variable.order = MAP_BYTEORDER_ABCDEFGH;
                } else if (strcmp(order_temp, "hgfedcba") == 0) {
                    current_variable.order = MAP_BYTEORDER_HGFEDCBA;
                }
                free(order_temp);
            }
            mbuf_append(&variable_buffer,&current_variable,sizeof(struct mb_variable));
            variable_counter++;
        }    
    }
   
    free(map);
    free(keys);

    if (variable_counter == 0) {
        mbuf_free(&variable_buffer);
        return false;
    }
    
    LOG(LL_DEBUG, ("%d valid variables detected, next step sorting",variable_counter));    

    //Ensure that variables are ordered by address
    qsort(variable_buffer.buf,variable_counter,sizeof(struct mb_variable),compare_modbus_variables);
  
    //Derive requests from variables considering limitation of maximum consecutive registers (block)
    uint16_t request_counter = 0;
    struct mbuf request_buffer;
    mbuf_init(&request_buffer,sizeof(struct mb_request));

    //Create Requests
    for(int i=0;i<variable_counter;i++){
        struct mb_request* current_request;
        if(i == 0){
            current_request = ((struct mb_request*) request_buffer.buf); 
        }else{
            current_request = ((struct mb_request*) request_buffer.buf)+(request_counter-1);
        }
        struct mb_variable* current_variable = ((struct mb_variable*) variable_buffer.buf) + i;
       
        if (i == 0 || (current_variable->address+current_variable->registers)-current_request->start > job->block){
            struct mb_request new_request = {current_variable->address,(uint16_t)current_variable->registers,1,NULL};
          
            mbuf_append(&request_buffer,&new_request,sizeof(struct mb_request));
      
            LOG(LL_DEBUG, ("Processed variable %s: new job new started:%d, new qty:%d", current_variable->key, new_request.start,  new_request.qty));
            request_counter++;
        }else{
            current_request->qty = (current_variable->address+current_variable->registers)-current_request->start;
            current_request->variables_count++;
            LOG(LL_DEBUG, ("Processed variable %s: next variable in existing job new start:%d, new qty:%d", current_variable->key, current_request->start,  current_request->qty));
        }
    }
    
    //Copy Variables
    for(int i=0;i<request_counter;i++){
        struct mb_request* current_request = ((struct mb_request*) request_buffer.buf)+i;
        current_request->variables = (struct mb_variable*) malloc(sizeof(struct mb_variable)*current_request->variables_count);
        memcpy(current_request->variables,((struct mb_variable*) variable_buffer.buf) + i,current_request->variables_count*sizeof(struct mb_variable));
    }
    mbuf_free(&variable_buffer);
    
    job->requests_count = request_counter;
    job->requests_next = request_counter;
    job->requests = (struct mb_request*) malloc(sizeof(struct mb_request)*request_counter);
    memcpy(job->requests,request_buffer.buf,sizeof(struct mb_request)*request_counter);
    mbuf_free(&request_buffer);
    return true;
}

static void rpc_modbus_read_keys_handler(struct mg_rpc_request_info* ri, void* cb_arg,
                                    struct mg_rpc_frame_info* fi, struct mg_str args) {
    //Get arguments from RPC call
    LOG(LL_INFO, ("Modbus.ReadKeys rpc called, payload: %.*s", args.len, args.p));
    int func = -1, id = -1, block = 4;
    char *keys=NULL, *map_file = NULL, *map = NULL;
    json_scanf(args.p, args.len, ri->args_fmt, &func, &id, &block, &keys, &map_file, &map);

    //Check essential arguments
    if (!(func == FUNC_READ_COILS || func == FUNC_READ_DISCRETE_INPUTS || func == FUNC_READ_HOLDING_REGISTERS || func == FUNC_READ_INPUT_REGISTERS)) {
        mg_rpc_send_errorf(ri, 400, "Unsupported function code");
        goto out;
    }
    if (id <= 0) {
        mg_rpc_send_errorf(ri, 400, "Slave id is required");
        goto out;
    }

    if (keys == NULL || strlen(keys) == 0) {
        mg_rpc_send_errorf(ri, 400, "Keys empty");
        goto out;
    }

    if(map == NULL && map_file == NULL){
        mg_rpc_send_errorf(ri, 400, "Map empty");
        goto out;
    }
    //Load map from file if not supplied in request
    if(map_file != NULL){
        map = json_fread(map_file);
        if (map == NULL) {
            LOG(LL_ERROR, ("Error reading modbus json map file"));
            mg_rpc_send_errorf(ri, 400, "Unable to execute modbus request");
            goto out;
        }
        free(map_file);
    }

    //Prepare MB job
    struct mb_job current_job =  {(uint8_t)func,(uint8_t)id,(uint8_t)block,NULL,0,0,NULL,rpc_send_job_response_cb,ri};
    struct mb_job* current_job_ptr = (struct mb_job*) malloc(sizeof(struct mb_job));
    memcpy(current_job_ptr,&current_job,sizeof(struct mb_job));

    if (!mb_init_job(current_job_ptr,map,keys)) {
        mg_rpc_send_errorf(ri, 400, "Could not init job");
        goto out;
    }
    
    if(!mb_next_request(current_job_ptr)){
        mg_rpc_send_errorf(ri, 400, "Unable to execute first modbus request");
        free(current_job_ptr);
    }

//Cast to void in case of errors in RPC request
out:
    (void)cb_arg;
    (void)fi;
    return;
}

static void rpc_modbus_get_keys_handler(struct mg_rpc_request_info* ri, void* cb_arg,
                                    struct mg_rpc_frame_info* fi, struct mg_str args) {
    //Get arguments from RPC call
    LOG(LL_INFO, ("Modbus.GetKeys rpc called, payload: %.*s", args.len, args.p));
    char *map_file = NULL, *map = NULL;
    json_scanf(args.p, args.len, ri->args_fmt, &map_file);

    //Check arguments
    if (map_file == NULL || strlen(map_file) == 0) {
        mg_rpc_send_errorf(ri, 400, "Map file empty or not found on device");
        goto out;
    }else{
        map = json_fread(map_file);
        if (map == NULL) {
            LOG(LL_ERROR, ("Error reading modbus json map file"));
            mg_rpc_send_errorf(ri, 400, "map file not found");
            goto out;
        }
        free(map_file);
    }

    void* h = NULL;
    struct json_token attr_name, attr_info;
    struct mbuf fullResponse;
    mbuf_init(&fullResponse,sizeof(char));
    mbuf_append(&fullResponse,"[",1);

    while ((h = json_next_key(map, strlen(map), h, ".", &attr_name, &attr_info)) != NULL) {
        mbuf_append(&fullResponse,"\"",1);
        mbuf_append(&fullResponse,attr_name.ptr,attr_name.len);
        mbuf_append(&fullResponse,"\", ",3);
    } 
    mbuf_insert(&fullResponse,fullResponse.len-2,"]",2);
    LOG(LL_INFO, ("Sent response: %s", (char*)fullResponse.buf));
    mg_rpc_send_responsef(ri, "{data:%s}", (char*)fullResponse.buf); 
    mbuf_free(&fullResponse);
    free(map);
    out:
    (void)cb_arg;
    (void)fi;
    return;                 
}

static void rpc_modbus_write_key_handler(struct mg_rpc_request_info* ri, void* cb_arg,
                                    struct mg_rpc_frame_info* fi, struct mg_str args){
    LOG(LL_INFO, ("Modbus.GetKeys rpc called, payload: %.*s", args.len, args.p));
    char *key = NULL, *map = NULL, *map_file = NULL;
    json_scanf(args.p, args.len, ri->args_fmt, &key,&map_file);
    //Check arguments
    if (map_file == NULL || strlen(map_file) == 0) {
        mg_rpc_send_errorf(ri, 400, "Map file empty or not found on device");
        goto out;
    }else{
        map = json_fread(map_file);
        if (map == NULL) {
            LOG(LL_ERROR, ("Error reading modbus json map file"));
            mg_rpc_send_errorf(ri, 400, "map file not found");
            goto out;
        }
        free(map_file);
    }
    if (key == NULL || strlen(key) == 0) {
        mg_rpc_send_errorf(ri, 400, "Map file empty or not found on device");
        goto out;
    }

    char* searchpat;
    mg_asprintf(&searchpat,0,"{%s:%%T}",key);       
    struct json_token res_token;
    if(json_scanf(map,strlen(map),searchpat,&res_token)>0){
        mg_rpc_send_responsef(ri, "{data:%s}", res_token.ptr); 
    }
    
    out:
    (void)cb_arg;
    (void)fi;
    return;  
} 

bool mgos_modbus_init(void) {
    LOG(LL_DEBUG, ("Initializing modbus"));
    if (!mgos_sys_config_get_modbus_enable())
        return true;
    if (!mgos_modbus_create(&mgos_sys_config.modbus)) {
        return false;
    }
    /*mg_rpc_add_handler(mgos_rpc_get_global(), "Modbus.Read",
                       "{func: %d, id:%d, start:%d, qty:%d, filename:%Q, json_map:%Q}",
                       rpc_modbus_read_handler, NULL);*/

    mg_rpc_add_handler(mgos_rpc_get_global(), "Modbus.GetKeys",
                       "{filename:%Q}",
                       rpc_modbus_get_keys_handler, NULL);
    
    mg_rpc_add_handler(mgos_rpc_get_global(), "Modbus.ReadKeys",
                       "{func: %d, id:%d, block:%d, keys:%Q, filename:%Q, json_map:%Q}",
                       rpc_modbus_read_keys_handler, NULL);
    
    mg_rpc_add_handler(mgos_rpc_get_global(), "Modbus.WriteKey",
                       "{id:%d, key:%Q, map:%Q}",
                       rpc_modbus_write_key_handler, NULL);

    return true;
}
