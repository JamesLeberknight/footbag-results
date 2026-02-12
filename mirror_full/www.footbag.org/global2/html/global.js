// Note: this is pre-jquery.

function FootbagOrgInit()
{
}

function HideMenu()
{
  mainMenu = document.getElementById("MainMenu");
  mainMenu.style.visibility = "hidden";
}

function ShowMenu()
{
  mainMenu = document.getElementById("MainMenu");
  mainMenu.style.visibility = "hidden";
}

function ExpandBox(containerName, newWidth, newHeight)
{
  container = document.getElementById(containerName);

//  if (container == null)
//    container = document.getElementByClass(containerName);

  if (container == null)
    return;

  if (newWidth != 0)
    container.style.width = newWidth;

  if (newHeight != 0)
    container.style.height = newHeight;
}

function ShowVideo(video, containerName, video)
{
  container = document.getElementById(containerName);
//  container.innerHTML = '<embed src="/newgallery/show/rclipper-eric?Mode=popup"';
  container.innerHTML = decodeURI(video);
}
