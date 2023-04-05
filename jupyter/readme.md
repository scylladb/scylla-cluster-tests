# What is Jupyter Notebook/Jupyterlab

## Jupyter Notebook

Jupyter Notebook is an open-source web application that allows you to create and share interactive documents containing live code, equations, visualizations, and narrative text. It is primarily used for data analysis, scientific computing, and machine learning.

The core component of Jupyter Notebook is the Notebook itself, which is a JSON file containing an ordered list of cells. There are several types of cells:

1. Code cells: These cells contain executable code, and when run, they display the output (e.g., text, plots, or tables) directly below the cell.
2. Markdown cells: These cells contain formatted text written in Markdown, a lightweight markup language. You can use them to provide explanations, add equations using LaTeX, or create section headings to organize your work.
3. Raw cells: These cells contain plain text that is not processed by the Notebook, and they're often used for code or text that should be included in the final document but not executed or rendered.

Jupyter Notebooks provide several advantages:

1. Interactive computing: They enable you to iteratively write and test your code, making debugging and experimentation much easier.
2. Collaboration: You can share your Notebooks with others, allowing them to reproduce your results, modify your code, or provide feedback.
3. Documentation: Combining code, visualizations, and narrative text in one place allows you to create comprehensive, self-contained, and understandable documents.

## Jupyterlab

JupyterLab is the next-generation web-based interface for Project Jupyter, providing a flexible and extensible environment for working with Jupyter Notebooks, code editors, and data visualization tools. It features a customizable user interface with multiple panels and tabs, an integrated text editor, a file explorer, terminal access, and a robust extension system. JupyterLab enhances the Jupyter Notebook experience, catering to users who require a more comprehensive workspace for data science, machine learning, or software development tasks.

# How to use it for SCT development

The main benefit of using Notebook is that it is interactive - so once test cluster is created you can play with it interactively, create new code, test it, correct and run over and over again without need to start whole framework again.

## Steps

Steps explain how to use it on SCT runner. There’s also possibility to run it locally when using k8s cluster (steps are similar, or you can use Jupyter Notebook support in Pycharm Professional or VS Code)

1. Create sct-runner and log in to it with ssh

    ```bash
    create sct-runner
    hydra create-runner-instance -r eu-west-1 -z a -c aws -t xxxxxxxx-aef7-4257-b7f5-f45b980abaaa -d 600
    ssh -i ~/.ssh/scylla-qa-ec2 ubuntu@<sct-runner-ip>
    ```
    Next steps run on SCT-runner
2. Git clone SCT

    ```bash
    cd ~
    git clone https://github.com/scylladb/scylla-cluster-tests.git
    ```

3. Configure AWS credentials

    ```bash
    aws configure
    # answer questions
    ```

4. Install and run Jupyterlab

    ```bash
    cd scylla-cluster-tests
    docker/env/hydra.sh /bin/bash
    # inside the container:
    pip install jupyterlab
    cd ~/scylla-cluster-tests/
    python -m jupyterlab --NotebookApp.token=''
    ```

5. Example output with connection details when token is provided (or `--NotebookApp.token` removed from command):

    ```bash
    To access the server, open this file in a browser:
            file:///home/ubuntu/.local/share/jupyter/runtime/jpserver-12-open.html
        Or copy and paste one of these URLs:
            http://localhost:8888/lab?token=bc1217d11fe892bb2c5a2f496e2ddab6639d7458b0b624da
            http://127.0.0.1:8888/lab?token=bc1217d11fe892bb2c5a2f496e2ddab6639d7458b0b624
    ```

6. On local machine create ssh tunnel to jupyterlab

    ```bash
    # create ssh tunnel
    ssh -N -f -L 127.0.0.1:8888:127.0.0.1:8888 -i ~/.ssh/scylla-qa-ec2 ubuntu@<sct-runner-ip>
    ```

7. Open web browser (or attach to Pycharm) this link:

    `http://localhost:8888/lab` or url from the output

7. Example use

    Run example jupyter notebooks in this directory
