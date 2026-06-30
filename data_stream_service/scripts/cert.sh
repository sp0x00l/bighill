#! /usr/bin/env sh

if [ ! -d "build" ]; then
  mkdir -p build
else 
  cd build
  if [ -f "*.pem" ]; then
    rm *.pem
  fi
  cd ..
fi


# 1. Generate CA's private key and self-signed certificate
openssl req -x509 -newkey rsa:4096 -days 365 -nodes -keyout ./build/ca-key.pem -out ./build/ca-cert.pem -subj "/CN=bighill"
echo "CA's self-signed certificate"
openssl x509 -in ./build/ca-cert.pem -noout -text
openssl req -newkey rsa:4096 -nodes -keyout ./build/server-key.pem -out ./build/server-req.pem -subj "/CN=bighill"
openssl x509 -req -in ./build/server-req.pem -days 60 -CA ./build/ca-cert.pem -CAkey ./build/ca-key.pem -CAcreateserial -out ./build/server-cert.pem # -extfile server-ext.cnf
 
echo "Server's signed certificate"
openssl x509 -in ./build/server-cert.pem -noout -text

