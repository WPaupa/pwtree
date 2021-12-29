#include "HashMap.h"
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>
#include "Tree.h"


int main(void)
{
    Tree *t = tree_new();
    tree_create(t, "/a/");
    tree_create(t, "/a/b/");
    tree_move(t, "/a/b/", "/x/");
    printf("%s\n", tree_list(t, "/"));
    tree_remove(t, "/a/");
    printf("%s\n", tree_list(t, "/"));
    tree_remove(t, "/x/");
    tree_free(t);
    return 0;
}