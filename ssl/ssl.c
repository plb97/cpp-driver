/*
  This is free and unencumbered software released into the public domain.

  Anyone is free to copy, modify, publish, use, compile, sell, or
  distribute this software, either in source code form or as a compiled
  binary, for any purpose, commercial or non-commercial, and by any
  means.

  In jurisdictions that recognize copyright laws, the author or authors
  of this software dedicate any and all copyright interest in the
  software to the public domain. We make this dedication for the benefit
  of the public at large and to the detriment of our heirs and
  successors. We intend this dedication to be an overt act of
  relinquishment in perpetuity of all present and future rights to this
  software under copyright law.

  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
  IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
  OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
  ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
  OTHER DEALINGS IN THE SOFTWARE.

  For more information, please refer to <http://unlicense.org/>
*/

#include <stdio.h>
#include <openssl/ssl.h>
#include <cassandra.h>

int load_trusted_cert_file(const char* cert_file, CassSsl* ssl) {
  CassError rc;
  char* cert;
  long cert_size;
  size_t bytes_read;

  FILE *in = fopen(cert_file, "rb");
  if (in == NULL) {
    fprintf(stderr, "Error loading certificate file '%s'\n", cert_file);
    return 0;
  }

  fseek(in, 0, SEEK_END);
  cert_size = ftell(in);
  rewind(in);

  cert = (char*)malloc(cert_size);
  bytes_read = fread(cert, 1, cert_size, in);
  fclose(in);

  if (bytes_read == (size_t) cert_size) {
    rc = cass_ssl_add_trusted_cert_n(ssl, cert, cert_size);
    if (rc != CASS_OK) {
      fprintf(stderr, "Error loading SSL certificate: %s\n", cass_error_desc(rc));
      free(cert);
      return 0;
    }
  }

  free(cert);
  return 1;
}

int load_private_cert_file(const char* key_cert_file, CassSsl* ssl) {
    CassError rc;
    char* cert;
    long cert_size;
    size_t bytes_read;

    FILE *in = fopen(key_cert_file, "rb");
    if (in == NULL) {
        fprintf(stderr, "Error loading key certificate file '%s'\n", key_cert_file);
        return 0;
    }

    fseek(in, 0, SEEK_END);
    cert_size = ftell(in);
    rewind(in);

    cert = (char*)malloc(cert_size);
    bytes_read = fread(cert, 1, cert_size, in);
    fclose(in);

    if (bytes_read == (size_t) cert_size) {
        // Load PEM-formatted certificate data and size into cert and cert_size...
         rc = cass_ssl_set_cert_n(ssl, cert, cert_size);
         if (rc != CASS_OK) {
             fprintf(stderr, "Error loading SSL certificate: %s\n", cass_error_desc(rc));
             free(cert);
         return 0;
         }
    }
    free(cert);
    return 1;
}

int load_private_key_file(const char* key_file, const char* key_password, const size_t key_password_size, CassSsl* ssl) {
    CassError rc;
    size_t bytes_read;
    char* key = NULL;
    size_t key_size = 0;

    FILE *in = fopen(key_file, "rb");
    if (in == NULL) {
        fprintf(stderr, "Error loading private key file '%s'\n", key_file);
        return 0;
    }

    fseek(in, 0, SEEK_END);
    key_size = ftell(in);
    rewind(in);

    key = (char*)malloc(key_size);
    bytes_read = fread(key, 1, key_size, in);
    fclose(in);

    if (bytes_read == (size_t) key_size) {
        // A password is required when the private key is encrypted.
        // If the private key is NOT password protected use NULL.
        // Load PEM-formatted private key data and size into key and key_size...
        rc = cass_ssl_set_private_key_n(ssl, key, key_size, key_password, key_password_size);
        if (rc != CASS_OK) {
            fprintf(stderr, "Error setting private key: %s\n", cass_error_desc(rc));
            free(key);
            return 0;
        }
    }
    free(key);
    return 1;
}

char* file_name(const char* dir, const char* name) {
  int len = strlen(dir)+strlen(name);
  char* str = (char*)malloc(len+1);
  str[len] = 0;
  sprintf(str,"%s%s",dir,name);
  return str;
}
int main(int argc, char* argv[]) {
  /* Setup and connect to cluster */
  CassFuture* connect_future = NULL;
  CassCluster* cluster = cass_cluster_new();
  CassSession* session = cass_session_new();
  CassSsl* ssl = cass_ssl_new();
  char* hosts = "127.0.0.1";
  if (argc > 1) {
    hosts = argv[1];
  }
  const char* dir = strcat(getenv("HOME"),"/.ssl/");

  cass_cluster_set_contact_points(cluster, hosts);

  /* Only verify the certification and not the identity */
  cass_ssl_set_verify_flags(ssl, CASS_SSL_VERIFY_PEER_CERT);

  const char* server_cert_file = file_name(dir,"cassandra.cer.pem");
  if (!load_trusted_cert_file(server_cert_file, ssl)) {
    fprintf(stderr, "Failed to load certificate disabling peer verification\n");
    cass_ssl_set_verify_flags(ssl, CASS_SSL_VERIFY_NONE);
  }
  free(server_cert_file);

  const char* key_cert_file = file_name(dir,"driver.cer.pem");
  if (!load_private_cert_file(key_cert_file, ssl)) {
      fprintf(stderr, "Failed to load private certificate\n");
      cass_ssl_set_verify_flags(ssl, CASS_SSL_VERIFY_NONE);
  }
  free(key_cert_file);

  const char* key_password = NULL;
  const size_t key_password_size = 0;
  const char* key_file = file_name(dir,"driver.key.pem");
  if (!load_private_key_file(key_file, key_password, key_password_size, ssl)) {
      fprintf(stderr, "Failed to load private key\n");
      cass_ssl_set_verify_flags(ssl, CASS_SSL_VERIFY_NONE);
  }
  free(key_file);

  cass_cluster_set_ssl(cluster, ssl);

  connect_future = cass_session_connect(session, cluster);

  if (cass_future_error_code(connect_future) == CASS_OK) {
    CassFuture* close_future = NULL;

    /* Build statement and execute query */
    const char* query = "SELECT release_version FROM system.local";
    CassStatement* statement = cass_statement_new(query, 0);

    CassFuture* result_future = cass_session_execute(session, statement);

    if (cass_future_error_code(result_future) == CASS_OK) {
      /* Retrieve result set and get the first row */
      const CassResult* result = cass_future_get_result(result_future);
      const CassRow* row = cass_result_first_row(result);

      if (row) {
        const CassValue* value = cass_row_get_column_by_name(row, "release_version");

        const char* release_version;
        size_t release_version_length;
        cass_value_get_string(value, &release_version, &release_version_length);
        printf("release_version: '%.*s'\n", (int)release_version_length,
               release_version);
      }

      cass_result_free(result);
    } else {
      /* Handle error */
      const char* message;
      size_t message_length;
      cass_future_error_message(result_future, &message, &message_length);
      fprintf(stderr, "Unable to run query: '%.*s'\n", (int)message_length,
                                                            message);
    }

    cass_statement_free(statement);
    cass_future_free(result_future);

    /* Close the session */
    close_future = cass_session_close(session);
    cass_future_wait(close_future);
    cass_future_free(close_future);
  } else {
      /* Handle error */
      const char* message;
      size_t message_length;
      cass_future_error_message(connect_future, &message, &message_length);
      fprintf(stderr, "Unable to connect: '%.*s'\n", (int)message_length,
                                                          message);
  }

  cass_future_free(connect_future);
  cass_cluster_free(cluster);
  cass_session_free(session);
  cass_ssl_free(ssl);

  return 0;
}
