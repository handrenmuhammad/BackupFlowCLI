db = connect("mongodb://localhost:27017/admin");

// Create admin user
db.getSiblingDB("admin").createUser({
    user: "guest",
    pwd: "guest",
    roles: ["root"]
});
