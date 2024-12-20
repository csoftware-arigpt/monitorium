# monitorium

Monitoring toolkit for channel/chat analysis.

# Bot features

1. search for channels/chats
2. Analyze channel/chat by such criteria as:
   - Number of comments per month
   - Average number of reactions
   - Number of posts per month
3. Saving all posts to the database

# How-to

1. Install

- Install python3 and poetry on your pc

- ```
  git clone https://github.com/csoftware-arigpt/monitorium
  cd monitorium
  poetry install
  ```
- Set up mysql server, or use `docker_init.sh` to run mysql database server

- Add proxy list in `proxy.json`

2. Running

- Add pyrogram (!) sessions to folder with json and rename like this:

  ```
  user_id.json
  user_id.session
  ```

- Edit run.sh file

- Run
 ```
bash run.sh  
 ```

## License

GNU GPL v3. Check `LICENSE` for more information

@csoftware, 2024
