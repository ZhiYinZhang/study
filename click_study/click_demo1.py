#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2018/12/18 16:10
import click
import torch
@click.group()
def cli():
    pass

@click.command()
@click.option("--cuda/--no-cuda",default=True)
def train(cuda):
    if cuda:
        click.echo(click.style("启用cuda",fg="green"))
    if not cuda:
        click.echo(click.style("不启用cuda",fg="red"))
    use_cuda = cuda and torch.cuda.is_available()
    print(use_cuda)

cli.add_command(train,name='train')

if __name__=="__main__":

    cli()