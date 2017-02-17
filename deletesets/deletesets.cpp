#include <stdio.h>
#include <stdlib.h>
#include <malloc.h>
#include <string.h>
#include <ctype.h>

void RewriteSetprefix(char *setprefix);
void ParseBuf(char *buf, int buflength, char *setprefix);
bool DeleteSet(char *namespacename, char *setname);

int main(int argc, char *argv[])
{
  if (argc != 2)
  {
    printf(
      "cleansets 1.0 - Mark matching Aerospike sets for deletion.\n"
      "\n"
      "Usage: cleansets <setprefix>\n"
      "\n"
      "setprefix - Used to match set names. Caution: All non\n"
      "            alphanumeric/underscore characters will be removed from\n"
      "            the setprefix.\n"
      "\n"
      "Note: An actual reboot is required for the sets to actual be removed.\n"
      "      Will to-be-deleted sets be removed automatically when the 1023\n"
      "      limit is reached by new sets? This doesn't seem to be the case,\n"
      "      don't count on it.\n");
    return 1;
  }

  char *setprefix = argv[1];
  RewriteSetprefix(setprefix);
  printf("setprefix: '%s'\n", setprefix);


  char filename[] = "sets.txt";

  char command[1000];
  snprintf(command, 1000, "echo \"show sets\" | aql > %s", filename);
  printf("Running aql to retrieve all sets: '%s'\n", command);
  system(command);


  FILE *fh;

  if (!(fh = fopen(filename, "r")))
  {
    printf("Couldn't open file: '%s'\n", filename);
    return 1;
  }

  fseek(fh, 0, SEEK_END);
  long size = ftell(fh);

  char *buf = (char *)malloc(size);
  if (!buf)
  {
    printf("Out of memory: %ld\n", size);
    return 1;
  }

  fseek(fh, 0, SEEK_SET);

  printf("Reading: '%s'\n", filename);
  fread(buf, size, 1, fh);
  fclose(fh);

  ParseBuf(buf, size, setprefix);

  free(buf);

  printf("Rebooting...\n");
  //system("shutdown -r");

  printf("Done!");

  return 0;
}

void RewriteSetprefix(char *setprefix)
{
  char  *p2 = setprefix;
  for (char *p1 = setprefix; *p1; p1++)
  {
    if (isalnum(*p1) || *p1 == '_')
    {
      *p2 = *p1;
      p2++;
    }
  }
  *p2 = 0;
}

void ParseBuf(char *buf, int buflength, char *setprefix)
{
  int rowcount = 1;

  for (int i = 0; i < buflength; i++)
  {
    if (buf[i] == '\n')
    {
      rowcount++;
    }
  }

  printf("Rows: %d\n", rowcount);


  int totalsetcount = 0;

  char header[] = "| disable-eviction";
  int headerlength = strlen(header);

  int setprefixlength = strlen(setprefix);

  for (int i = 0; i < buflength; i++)
  {
    if (i > 1 && i < buflength - headerlength && buf[i - 1] == '\n' && buf[i] == '|' && memcmp(buf + i, header, headerlength))
    {
      totalsetcount++;
    }
  }

  printf("Found total sets: %d\n", totalsetcount);

  int setcount = 0;

  for (int i = 0; i < buflength; i++)
  {
    if (i > 1 && i < buflength - headerlength && buf[i - 1] == '\n' && buf[i] == '|' && memcmp(buf + i, header, headerlength))
    {
      char *namespacename, *setname;
      char colcount = 0;
      for (int j = i; j < buflength && buf[j] != '\n'; j++)
      {
        if (buf[j] == '|')
        {
          colcount++;

          if (colcount == 2)
          {
            namespacename = buf + j + 3;
          }
          if (colcount == 6)
          {
            setname = buf + j + 3;
          }
        }

        if (buf[j] == '\"')
        {
          buf[j] = 0;
        }
      }

      if (!strncmp(setname, setprefix, setprefixlength))
      {
        if (DeleteSet(namespacename, setname))
        {
          setcount++;
        }
      }
    }
  }


  printf("Deleted sets: %d/%d\n", setcount, totalsetcount);


  return;
}

bool DeleteSet(char *namespacename, char *setname)
{
  if (strlen(namespacename) > 100)
  {
    printf("Namespace name too long: '%s.%s'\n", namespacename, setname);
    return false;
  }
  if (strlen(setname) > 200)
  {
    printf("Set name too long: '%s.%s'\n", namespacename, setname);
    return false;
  }

  char name[1000];
  snprintf(name, 1000, "'%s.%s'.", namespacename, setname);

  char command[1000];
  snprintf(command, 1000, "asinfo -v \"set-config:context=namespace;id=%s;set=%s;set-delete=true;\"", namespacename, setname);

  printf("Deleting: %-50s Running: '%s'\n", name, command);
  system(command);

  return true;
}
