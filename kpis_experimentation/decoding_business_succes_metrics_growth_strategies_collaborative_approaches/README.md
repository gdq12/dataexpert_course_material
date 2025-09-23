### Set up Python Env with Docker 

1. define `GITHUB_FOLDER` in a `.envc` file in `dataexpert_intermediate_bootcamp_2025/setup_python/` directory

    - this is the absolute path to the local directory where github content is maintained

2. build the docker image that will be called upon by devcontainer

    ```
    cd dataexpert_intermediate_bootcamp_2025/setup_python

    docker build -t dataexpert_community_bootcamp:latest .
    ```

3. launch docker container useing devcontainer in VS code 

    ```
    cd dataexpert_intermediate_bootcamp_2025/setup_python

    source .envc

    code .

    # Yes to launch in contianer
    ```

### Setting up account in Statig

1. create a free account on `https://statsig.com/`

2. install the SDK

    ```
    pip3 install statsig-python-core

    ```
3. save the server key as `STATSIG_API_KEY` in `.devcontainer/.devcontainer`

4. provided lines of code by Statsig website to follow

    ```python
    # Initialize
    from statsig_python_core import Statsig, StatsigOptions # note underscores instead of hyphens in import

    statsig = Statsig("secret-key")
    statsig.initialize().wait()

    # or with StatsigOptions
    options = StatsigOptions()
    options.environment = "development"

    statsig = Statsig("secret-key", options)
    statsig.initialize().wait()


    # log events 
    from statsig_python_core import StatsigUser

    statsig.log_event(
        user=StatsigUser("user_id"),  # Replace with your user object
        event_name="add_to_cart",
        value="SKU_12345",
        metadata={
            "price": "9.99",
            "item_name": "diet_coke_48_pack"
        }
    )
    ```

### Setting up an experiment

1. on the experiment tab of the dashboard select `Create an Experiment`

    - Experiment name: `lesson_button`

    - hypothesis: `I think a green button is the best one`

    - ID type: `User ID`

2. select primary and secondary metrics that want to work with 

    - **some of the metrics are out of the box provided by statsig, but there are other that can be custom built**

    - primary: `dau(user)`(daily active users), `visited_signup(event_count)`

    - secondary: `core` (dau, mau_28d, new_dau, I7, weekly_stickiness, monthly_stickiness)

3. add parameter to `Groups and Parameters` with `Add Another Paramter`

    - paramter: `Button Color`

        + control group: `blue`

        + `Add group`: `red`, `green`, `orange`

    - parameter: `Paragraph Text`

        + control group: `Ocean wave`

        + test groups: `Red wedding`, `Fores green`, `Florida sunshine`

4. save all settings

5. `Start` to commence experiment

6. additional good to knows:

    * but have parameters defined before finalozing experiment setup, once the setup is complete cannot ammend parameters

    * define what % of users will be used for experiementation (and from which device)

    * from that value can distribute users to the different groups, will be calced according to assigned %

    * the event data from the experiments will appear in the `Pulse Results` tab of the experiment dashboard

### Launch Flask app 

1. in container, make sure in the same directory as python script and execute 

    ```
    python day1_lab.py
    ```

### Notes about the script 

* the app will be running in `http://127.0.0.1:5000`

* can jump through the IP addresses by adding the `@app.route()` from the script to the end of the address 

* the `hash_string = request.remote_addr` in line 53 generates the IP address 

* the following lines (lines 52-55), is responsible for assigning the user to random groups when `http://127.0.0.1:5000/tasks?random=true` is executed

    ```
    random_num = request.args.get('random')
    hash_string = request.remote_addr
    if random_num:
        hash_string = str(random.randint(0, 1000000))
    ```

* line 57: `color = statsig.get_experiment(StatsigUser(user_id), "button_color_v3").get("Button Color", "blue")` --> statsig.get_experiment(StatsigUser(user_id), "ExperimentName").get("AttributeToFetch", "defaultAttributeCharacteristic")

* create a signup page: this is code lines 34-47

* line 95: in tasks page `<a href="/signup">Go to Signup</a>` provides a hyper link for the user to go to the signup page

* for the `visited_signup` metric, need the `statsig.log_event(statsig_event)` in line 46 to track user activity 

* where the logging occurs plays a role in terms of specific the metric measurement occurs 

    - in this instance logging sign-up on the server side (like here) or on the client side can have a very different count 

    - server side logging is much easier 

    - client side logging tricker --> dont want to give users the capacity to log whatever they wish 

    - with client server, would have to provide oauth authentication authorization which is more complicated 

    - with client server get better fidelity data, more accurate eent logging 

* feature gate: 

    - dont get groups, only get Y/N

    - when have only 2 outcomes, then better to use 

    - can be easier to work with as opposed to experiments

    - more flexible to setup and maintain 

    - good whe working with hold-out groups 

    - can run more long term experiments 

    - get more data for stat sig 

    - when looking at the monitoring metrics stat over view, for a 95% confidence interval, when a bar has a wide range and overlaps the zero mid point, it means the metrics doesnt really have a significant affect

    - when lower/increase confidence interval --> change the stat sig of the results of a given metric 

    - each confidence interval comes with an alpha values (prob that hypothesis wrong) --> th higher the interval the decrease in the chance that being wrong

* there is a metric catalog under `Data` section of the UI

    - its all the metrics that have been used in any experiment and feature gate 

    - this is an organized way to keep oversight into which metrics to use when and how to consider them

    - this overview can be managed by using tags for example 

    - for instance can use tags to define guardrail metrics: metrics that when they perform poorly in experiments they should not be pushed to production 

* guardrail metrics must:

    - high quality 

    - not buggy 

    - very trusted 

    - very fresh 

### Notes on Statsig UI

* once experiment is launched and triggered the API a couple of times, then can visit the `Diagnostics` tab of the experimentation dashboard to see the events created

* can take a look at the raw event data that was produced by visiting `Events` in the `Data` part of the UI. This can be found on the toggled left bar

### Good to have links

* [Getting Started Guide](https://docs.statsig.com/sdks/quickstart/)

